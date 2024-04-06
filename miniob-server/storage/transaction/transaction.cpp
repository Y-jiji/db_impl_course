/* Copyright (c) 2021 Xie Meiyi(xiemeiyi@hust.edu.cn) and OceanBase and/or its
affiliates. All rights reserved. miniob is licensed under Mulan PSL v2. You can
use this software according to the terms and conditions of the Mulan PSL v2. You
may obtain a copy of Mulan PSL v2 at: http://license.coscl.org.cn/MulanPSL2 THIS
SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, EITHER
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2021/5/24.
//

#include <atomic>

#include <common/log/log.h>
#include <storage/common/field_meta.h>
#include <storage/common/record_manager.h>
#include <storage/common/table.h>
#include <storage/transaction/transaction.h>

static const uint32_t DELETED_FLAG_BIT_MASK   = 0x80000000;
static const uint32_t transaction_ID_BIT_MASK = 0x7FFFFFFF;

int32_t Transaction::default_transaction_id() { return 0; }

int32_t Transaction::next_transaction_id() {
    static std::atomic<int32_t> transaction_id;
    return ++transaction_id;
}

const char* Transaction::transaction_field_name() { return "__transaction"; }

AttrType    Transaction::transaction_field_type() { return INTS; }

int         Transaction::transaction_field_len() { return sizeof(int32_t); }

Transaction::Transaction() {}

Transaction::~Transaction() {}

ReturnCode Transaction::insert_record(Table* table, Record* record) {
    ReturnCode rc = ReturnCode::SUCCESS;
    // 先校验是否以前是否存在过(应该不会存在)
    Operation* old_oper = find_operation(table, record->rid);
    if (old_oper != nullptr) {
        return ReturnCode::GENERIC_ERROR; // error code
    }

    start_if_not_started();

    // 设置record中transaction_field为当前的事务号
    // set_record_transaction_id(table, record, transaction_id_, false);
    // 记录到operations中
    insert_operation(table, Operation::Type::INSERT, record->rid);
    return rc;
}

ReturnCode Transaction::delete_record(Table* table, Record* record) {
    ReturnCode rc = ReturnCode::SUCCESS;
    start_if_not_started();
    Operation* old_oper = find_operation(table, record->rid);
    if (old_oper != nullptr) {
        if (old_oper->type() == Operation::Type::INSERT) {
            delete_operation(table, record->rid);
            return ReturnCode::SUCCESS;
        } else {
            return ReturnCode::GENERIC_ERROR;
        }
    }
    set_record_transaction_id(table, *record, transaction_id_, true);
    insert_operation(table, Operation::Type::DELETE, record->rid);
    return rc;
}

void Transaction::set_record_transaction_id(Table* table, Record& record,
                                            int32_t transaction_id,
                                            bool    deleted) const {
    const FieldMeta* transaction_field =
        table->table_meta().transaction_field();
    int32_t* ptransaction_id =
        (int32_t*)(record.data + transaction_field->offset());
    if (deleted) {
        transaction_id |= DELETED_FLAG_BIT_MASK;
    }
    *ptransaction_id = transaction_id;
}

void Transaction::get_record_transaction_id(Table* table, const Record& record,
                                            int32_t& transaction_id,
                                            bool&    deleted) {
    const FieldMeta* transaction_field =
        table->table_meta().transaction_field();
    int32_t transaction =
        *(int32_t*)(record.data + transaction_field->offset());
    transaction_id = transaction & transaction_ID_BIT_MASK;
    deleted        = (transaction & DELETED_FLAG_BIT_MASK) != 0;
}

Operation* Transaction::find_operation(Table* table, const RID& rid) {
    std::unordered_map<Table*, OperationSet>::iterator table_operations_iter =
        operations_.find(table);
    if (table_operations_iter == operations_.end()) {
        return nullptr;
    }

    OperationSet&          table_operations = table_operations_iter->second;
    Operation              tmp(Operation::Type::UNDEFINED, rid);
    OperationSet::iterator operation_iter = table_operations.find(tmp);
    if (operation_iter == table_operations.end()) {
        return nullptr;
    }
    return const_cast<Operation*>(&(*operation_iter));
}

void Transaction::insert_operation(Table* table, Operation::Type type,
                                   const RID& rid) {
    OperationSet& table_operations = operations_[table];
    table_operations.emplace(type, rid);
}

void Transaction::delete_operation(Table* table, const RID& rid) {

    std::unordered_map<Table*, OperationSet>::iterator table_operations_iter =
        operations_.find(table);
    if (table_operations_iter == operations_.end()) {
        return;
    }

    Operation tmp(Operation::Type::UNDEFINED, rid);
    table_operations_iter->second.erase(tmp);
}

ReturnCode Transaction::commit() {
    ReturnCode rc = ReturnCode::SUCCESS;
    for (const auto& table_operations : operations_) {
        Table*              table         = table_operations.first;
        const OperationSet& operation_set = table_operations.second;
        for (const Operation& operation : operation_set) {

            RID rid;
            rid.page_num = operation.page_num();
            rid.slot_num = operation.slot_num();

            switch (operation.type()) {
            case Operation::Type::INSERT: {
                rc = table->commit_insert(this, rid);
                if (rc != ReturnCode::SUCCESS) {
                    // handle rc
                    LOG_ERROR("Failed to commit insert operation. rid=%d.%d, "
                              "rc=%d:%s",
                              rid.page_num, rid.slot_num, rc, strrc(rc));
                }
            } break;
            case Operation::Type::DELETE: {
                rc = table->commit_delete(this, rid);
                if (rc != ReturnCode::SUCCESS) {
                    // handle rc
                    LOG_ERROR("Failed to commit delete operation. rid=%d.%d, "
                              "rc=%d:%s",
                              rid.page_num, rid.slot_num, rc, strrc(rc));
                }
            } break;
            default: {
                LOG_PANIC("Unknown operation. type=%d", (int)operation.type());
            } break;
            }
        }
    }

    operations_.clear();
    transaction_id_ = 0;
    return rc;
}

ReturnCode Transaction::rollback() {
    ReturnCode rc = ReturnCode::SUCCESS;
    for (const auto& table_operations : operations_) {
        Table*              table         = table_operations.first;
        const OperationSet& operation_set = table_operations.second;
        for (const Operation& operation : operation_set) {

            RID rid;
            rid.page_num = operation.page_num();
            rid.slot_num = operation.slot_num();

            switch (operation.type()) {
            case Operation::Type::INSERT: {
                rc = table->rollback_insert(this, rid);
                if (rc != ReturnCode::SUCCESS) {
                    // handle rc
                    LOG_ERROR("Failed to rollback insert operation. rid=%d.%d, "
                              "rc=%d:%s",
                              rid.page_num, rid.slot_num, rc, strrc(rc));
                }
            } break;
            case Operation::Type::DELETE: {
                rc = table->rollback_delete(this, rid);
                if (rc != ReturnCode::SUCCESS) {
                    // handle rc
                    LOG_ERROR("Failed to rollback delete operation. rid=%d.%d, "
                              "rc=%d:%s",
                              rid.page_num, rid.slot_num, rc, strrc(rc));
                }
            } break;
            default: {
                LOG_PANIC("Unknown operation. type=%d", (int)operation.type());
            } break;
            }
        }
    }

    operations_.clear();
    transaction_id_ = 0;
    return rc;
}

ReturnCode Transaction::commit_insert(Table* table, Record& record) {
    set_record_transaction_id(table, record, 0, false);
    return ReturnCode::SUCCESS;
}

ReturnCode Transaction::rollback_delete(Table* table, Record& record) {
    set_record_transaction_id(table, record, 0, false);
    return ReturnCode::SUCCESS;
}

bool Transaction::is_visible(Table* table, const Record* record) {
    int32_t record_transaction_id;
    bool    record_deleted;
    get_record_transaction_id(table, *record, record_transaction_id,
                              record_deleted);

    // 0 表示这条数据已经提交
    if (0 == record_transaction_id ||
        record_transaction_id == transaction_id_) {
        return !record_deleted;
    }

    return record_deleted; // 当前记录上面有事务号，说明是未提交数据，那么如果有删除标记的话，就表示是未提交的删除
}

void Transaction::init_transaction_info(Table* table, Record& record) {
    set_record_transaction_id(table, record, transaction_id_, false);
}

void Transaction::start_if_not_started() {
    if (transaction_id_ == 0) {
        transaction_id_ = next_transaction_id();
    }
}