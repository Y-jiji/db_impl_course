/* Copyright (c) 2021 Xie Meiyi(xiemeiyi@hust.edu.cn) and OceanBase and/or its
affiliates. All rights reserved. miniob is licensed under Mulan PSL v2. You can
use this software according to the terms and conditions of the Mulan PSL v2. You
may obtain a copy of Mulan PSL v2 at: http://license.coscl.org.cn/MulanPSL2 THIS
SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, EITHER
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Meiyi & Wangyunlai on 2021/5/12.
//

#ifndef __OBSERVER_STORAGE_COMMON_TABLE_H__
#define __OBSERVER_STORAGE_COMMON_TABLE_H__

#include <storage/common/table_meta.h>

class DiskBufferPool;
class RecordFileHandler;
class ConditionFilter;
class DefaultConditionFilter;
struct Record;
struct RID;
class Index;
class IndexScanner;
class RecordDeleter;
class Transaction;

class Table {
    public:
    Table();
    ~Table();

    /**
     * 创建一个表
     * @param path 元数据保存的文件(完整路径)
     * @param name 表名
     * @param base_dir 表数据存放的路径
     * @param attribute_count 字段个数
     * @param attributes 字段
     */
    ResultCode create(const char* path, const char* name, const char* base_dir,
              int attribute_count, const AttrInfo attributes[]);

    /**
     * 打开一个表
     * @param meta_file 保存表元数据的文件完整路径
     * @param base_dir 表所在的文件夹，表记录数据文件、索引数据文件存放位置
     */
    ResultCode open(const char* meta_file, const char* base_dir);
    ResultCode destroy(const char* dir);
    ResultCode insert_record(Transaction* transaction, int value_num, const Value* values);
    ResultCode update_record(Transaction* transaction, const char* attribute_name, const Value* value,
                     int condition_num, const Condition conditions[],
                     int* updated_count);
    ResultCode delete_record(Transaction* transaction, ConditionFilter* filter, int* deleted_count);

    ResultCode scan_record(Transaction* transaction, ConditionFilter* filter, int limit, void* context,
                   void (*record_reader)(const char* data, void* context));

    ResultCode create_index(Transaction* transaction, const char* index_name,
                    const char* attribute_name);

    public:
    const char*      name() const;

    const TableMeta& table_meta() const;

    ResultCode               sync();

    public:
    ResultCode commit_insert(Transaction* transaction, const RID& rid);
    ResultCode commit_delete(Transaction* transaction, const RID& rid);
    ResultCode rollback_insert(Transaction* transaction, const RID& rid);
    ResultCode rollback_delete(Transaction* transaction, const RID& rid);

    private:
    ResultCode scan_record(Transaction* transaction, ConditionFilter* filter, int limit, void* context,
                   ResultCode (*record_reader)(Record* record, void* context));
    ResultCode scan_record_by_index(Transaction* transaction, IndexScanner* scanner,
                            ConditionFilter* filter, int limit, void* context,
                            ResultCode (*record_reader)(Record* record, void* context));
    IndexScanner* find_index_for_scan(const ConditionFilter* filter);
    IndexScanner* find_index_for_scan(const DefaultConditionFilter& filter);

    ResultCode            insert_record(Transaction* transaction, Record* record);
    ResultCode            delete_record(Transaction* transaction, Record* record);

    private:
    friend class RecordUpdater;
    friend class RecordDeleter;

    ResultCode insert_entry_of_indexes(const char* record, const RID& rid);
    ResultCode delete_entry_of_indexes(const char* record, const RID& rid,
                               bool error_on_not_exists);

    private:
    ResultCode init_record_handler(const char* base_dir);
    ResultCode make_record(int value_num, const Value* values, char*& record_out);

    private:
    Index* find_index(const char* index_name) const;

    private:
    std::string         base_dir_;
    TableMeta           table_meta_;
    DiskBufferPool*     data_buffer_pool_; /// 数据文件关联的buffer pool
    int                 file_id_;
    RecordFileHandler*  record_handler_; /// 记录操作
    std::vector<Index*> indexes_;
};

#endif // __OBSERVER_STORAGE_COMMON_TABLE_H__