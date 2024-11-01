/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "sm_manager.h"

#include <sys/stat.h>
#include <unistd.h>

#include <fstream>

#include "index/ix.h"
#include "record/rm.h"
#include "record_printer.h"

/**
 * @description: 判断是否为一个文件夹
 * @return {bool} 返回是否为一个文件夹
 * @param {string&} db_name 数据库文件名称，与文件夹同名
 */
bool SmManager::is_dir(const std::string& db_name) {
    struct stat st;
    return stat(db_name.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

/**
 * @description: 创建数据库，所有的数据库相关文件都放在数据库同名文件夹下
 * @param {string&} db_name 数据库名称
 */
void SmManager::create_db(const std::string& db_name) {

    
    if (is_dir(db_name)) {
        throw DatabaseExistsError(db_name);
    }

    //为数据库创建一个子目录
    std::string cmd = "mkdir " + db_name;
    if (system(cmd.c_str()) < 0) {  // 创建一个名为db_name的目录
        throw UnixError();
    }
    if (chdir(db_name.c_str()) < 0) {  // 进入名为db_name的目录
        throw UnixError();
    }


    //创建系统目录
    DbMeta *new_db = new DbMeta();
    new_db->name_ = db_name;

    // 注意，此处ofstream会在当前目录创建(如果没有此文件先创建)和打开一个名为DB_META_NAME的文件
    std::ofstream ofs(DB_META_NAME);

    // 将new_db中的信息，按照定义好的operator<<操作符，写入到ofs打开的DB_META_NAME文件中
    ofs << *new_db;  // 注意：此处重载了操作符<<

    delete new_db;

    // 创建日志文件
    disk_manager_->create_file(LOG_FILE_NAME);

    // 回到根目录
    if (chdir("..") < 0) {
        throw UnixError();
    }
}

/**
 * @description: 打开数据库，找到数据库对应的文件夹，并加载数据库元数据和相关文件
 * @param {string&} db_name 数据库名称，与文件夹同名
 */
void SmManager::open_db(const std::string& db_name) {

    

    // 检查数据库目录是否存在
    if (!is_dir(db_name)) {
        throw DatabaseNotFoundError(db_name);
    }

    // 数据库已经打开
    if (!db_.name_.empty()) {
        throw DatabaseExistsError(db_name);
    }
    
    // 进入数据库目录
    if (chdir(db_name.c_str()) < 0) {
        throw UnixError();
    }

    // 读取数据库元数据
    std::ifstream ifs(DB_META_NAME);
    if (!ifs.is_open()) {
        throw UnixError();
    }

    // 从文件中加载元数据
    ifs >> db_;  // 使用重载的操作符>>
    ifs.close();

    // 加载所有表文件和索引文件

    for (auto &entry : db_.tabs_) {
        TabMeta tab = entry.second;
        fhs_.emplace(tab.name, rm_manager_->open_file(tab.name));
        for (auto &index : tab.indexes) {
            std::string index_name = ix_manager_->get_index_name(tab.name, index.cols);
            ihs_.emplace(tab.name, ix_manager_->open_index(index_name, index.cols));
        }
    }



}
/**
 * @description: 删除数据库，同时需要清空相关文件以及数据库同名文件夹
 * @param {string&} db_name 数据库名称，与文件夹同名
 */
void SmManager::drop_db(const std::string& db_name) {
    if (!is_dir(db_name)) {
        throw DatabaseNotFoundError(db_name);
    }
    std::string cmd = "rm -r " + db_name;
    if (system(cmd.c_str()) < 0) {
        throw UnixError();
    }
}


/**
 * @description: 把数据库相关的元数据刷入磁盘中
 */
void SmManager::flush_meta() {
    // 默认清空文件
    std::ofstream ofs(DB_META_NAME);
    ofs << db_;
}

/**
 * @description: 关闭数据库并把数据落盘
 */
void SmManager::close_db() {

    flush_meta();
    std::ofstream ofs(DB_META_NAME);
    db_.name_.clear();
    db_.tabs_.clear();
     // Close all record files
     // 记录文件落盘
    std::cout << "before file: " << std::endl;
    for (auto &entry : fhs_) {
        rm_manager_->close_file(entry.second.get());
    }
	fhs_.clear();
    
	// Close all index files
	// ...   
    // 记录文件落盘
    std::cout << "before file: " << std::endl;
    for (auto &entry : ihs_) {
        ix_manager_->close_index(entry.second.get());
    }
    ihs_.clear();
    if (chdir("..") < 0) {
        throw UnixError();
    }
}

/**
 * @description: 显示所有的表,通过测试需要将其结果写入到output.txt,详情看题目文档
 * @param {Context*} context 
 */
void SmManager::show_tables(Context* context) {
    std::fstream outfile;
    printf("success3\n");
    outfile.open("output.txt", std::ios::out | std::ios::app);
    outfile << "| Tables |\n";
    RecordPrinter printer(1);
    printer.print_separator(context);
    printer.print_record({"Tables"}, context);
    printer.print_separator(context);
    for (auto &entry : db_.tabs_) {
        auto &tab = entry.second;
        printer.print_record({tab.name}, context);
        outfile << "| " << tab.name << " |\n";
    }
    printer.print_separator(context);
    outfile.close();
}

/**
 * @description: 显示表的元数据
 * @param {string&} tab_name 表名称
 * @param {Context*} context 
 */
void SmManager::desc_table(const std::string& tab_name, Context* context) {
    TabMeta &tab = db_.get_table(tab_name);

    std::vector<std::string> captions = {"Field", "Type", "Index"};
    RecordPrinter printer(captions.size());
    // Print header
    printer.print_separator(context);
    printer.print_record(captions, context);
    printer.print_separator(context);
    // Print fields
    for (auto &col : tab.cols) {
        std::vector<std::string> field_info = {col.name, coltype2str(col.type), col.index ? "YES" : "NO"};
        printer.print_record(field_info, context);
    }
    // Print footer
    printer.print_separator(context);
}

/**
 * @description: 创建表
 * @param {string&} tab_name 表的名称
 * @param {vector<ColDef>&} col_defs 表的字段
 * @param {Context*} context 
 */
void SmManager::create_table(const std::string& tab_name, const std::vector<ColDef>& col_defs, Context* context) {
    if (db_.is_table(tab_name)) {
        throw TableExistsError(tab_name);
    }
    // Create table meta
    int curr_offset = 0;
    TabMeta tab;
    tab.name = tab_name;
    for (auto &col_def : col_defs) {
        ColMeta col = {.tab_name = tab_name,
                       .name = col_def.name,
                       .type = col_def.type,
                       .len = col_def.len,
                       .offset = curr_offset,
                       .index = false};
        curr_offset += col_def.len;
        tab.cols.push_back(col);
    }
    // Create & open record file
    int record_size = curr_offset;  // record_size就是col meta所占的大小（表的元数据也是以记录的形式进行存储的）
    rm_manager_->create_file(tab_name, record_size);
    db_.tabs_[tab_name] = tab;
    // fhs_[tab_name] = rm_manager_->open_file(tab_name);
    fhs_.emplace(tab_name, rm_manager_->open_file(tab_name));

    flush_meta();
    printf("success1\n");
}

/**
 * @description: 删除表
 * @param {string&} tab_name 表的名称
 * @param {Context*} context
 */
void SmManager::drop_table(const std::string& tab_name, Context* context) {
    TabMeta &tab = db_.get_table(tab_name);
    rm_manager_->close_file(fhs_[tab_name].get());
    rm_manager_->destroy_file(tab_name);
    for (auto &index : tab.indexes) {
        std::string index_name = ix_manager_->get_index_name(tab_name, index.cols);
        ix_manager_->close_index(ihs_[index_name].get());
        ix_manager_->destroy_index(tab_name, index.cols);
        ihs_.erase(index_name);
    }
    db_.tabs_.erase(tab_name); 
    fhs_.erase(tab_name);
    printf("success2\n");
    flush_meta();
}

/**
 * @description: 创建索引
 * @param {string&} tab_name 表的名称
 * @param {vector<string>&} col_names 索引包含的字段名称
 * @param {Context*} context
 */
void SmManager::create_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context) {
    auto &&ix_name = ix_manager_->get_index_name(tab_name, col_names);
    if (disk_manager_->is_file(ix_name)) {
        throw IndexExistsError(tab_name, col_names);
    }
    // 异常情况检查放前面
    auto &table_meta = db_.get_table(tab_name);

    // 表级 S 锁
    // 建立索引要读表上的所有记录，所以申请表级读锁
    // if (context != nullptr && context->lock_mgr_ != nullptr) {
    //     context->lock_mgr_->lock_shared_on_table(context->txn_, fhs_[tab_name]->GetFd());
    // }

    std::vector<ColMeta> col_metas;
    col_metas.reserve(col_names.size());
    auto total_len = 0;
    for (auto &col_name: col_names) {
        col_metas.emplace_back(*table_meta.get_col(col_name));
        total_len += col_metas.back().len;
    }

    // auto ix_name = std::move(ix_manager_->get_index_name(tab_name, col_names));
    ix_manager_->create_index(tab_name, col_metas);
    auto &&ih = ix_manager_->open_index(tab_name, col_names);
    auto &&fh = fhs_[tab_name];
    int offset = 0;
    char *key = new char[total_len];
    for (auto &&scan = std::make_unique<RmScan>(fh.get()); !scan->is_end(); scan->next()) {
        auto &&rid = scan->rid();
        auto &&record = fh->get_record(rid, context);
        offset = 0;
        for (auto &col_meta: col_metas) {
            memcpy(key + offset, record->data + col_meta.offset, col_meta.len);
            offset += col_meta.len;
        }
        // 插入B+树
        if (ih->insert_entry(key, rid, context->txn_) == IX_NO_PAGE) {
            // 释放内存
            delete []key;
            // 重复了
            ix_manager_->close_index(ih.get());
            ix_manager_->destroy_index(tab_name, col_names);
            // drop_index(tab_name, col_names, context);
            return;
        }
    }
    // 释放内存
    delete []key;

    IndexMeta* index_meta = new IndexMeta();  // 动态分配
    index_meta->tab_name = tab_name;
    index_meta->col_tot_len = total_len;
    index_meta->col_num = col_names.size();
    index_meta->cols = col_metas;

    // 更新表元索引数据
    table_meta.indexes.push_back(*index_meta);
    // 插入索引句柄
    ihs_[std::move(ix_name)] = std::move(ih);
    // 持久化
    flush_meta();

}

   

/**
 * @description: 删除索引
 * @param {string&} tab_name 表名称
 * @param {vector<string>&} col_names 索引包含的字段名称
 * @param {Context*} context
 */
void SmManager::drop_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context) {
     auto &table_meta = db_.get_table(tab_name);
    auto &&ix_name = ix_manager_->get_index_name(tab_name, col_names);
    if (!disk_manager_->is_file(ix_name)) {
        throw IndexNotFoundError(tab_name, col_names);
    }

    // 表级 S 锁
    // 删除索引时只允许对表读操作，写操作可能会误写将被删除的索引，所以申请表级读锁
    // if (context != nullptr) {
    //     context->lock_mgr_->lock_shared_on_table(context->txn_, fhs_[tab_name]->GetFd());
    // }

    ix_manager_->close_index(ihs_[ix_name].get());
    ix_manager_->destroy_index(tab_name, col_names);
    ihs_.erase(ix_name);
    auto index = table_meta.get_index_meta(col_names)[0];
    table_meta.indexes.erase(
        std::remove_if(table_meta.indexes.begin(), table_meta.indexes.end(),
                       [&index](const IndexMeta& idx) {
                           return idx == index;  // 根据相等判断删除
                       }),
        table_meta.indexes.end()
    );
    // 持久化
    flush_meta();
    
}

/**
 * @description: 删除索引
 * @param {string&} tab_name 表名称
 * @param {vector<ColMeta>&} 索引包含的字段元数据
 * @param {Context*} context
 */
void SmManager::drop_index(const std::string& tab_name, const std::vector<ColMeta>& cols, Context* context) {
    auto &table_meta = db_.get_table(tab_name);
    auto &&ix_name = ix_manager_->get_index_name(tab_name, cols);
    std::vector<std::string> col_names;
    col_names.reserve(cols.size());
    for (auto &col: cols) {
        col_names.emplace_back(col.name);
    }
    if (!disk_manager_->is_file(ix_name)) {
        throw IndexNotFoundError(tab_name, col_names);
    }

    // 表级 S 锁
    // 删除索引时只允许对表读操作，写操作可能会误写将被删除的索引，所以申请表级读锁
    // if (context != nullptr) {
    //     context->lock_mgr_->lock_shared_on_table(context->txn_, fhs_[tab_name]->GetFd());
    // }

    ix_manager_->close_index(ihs_[ix_name].get());
    ix_manager_->destroy_index(tab_name, cols);
    ihs_.erase(ix_name);
    auto index = table_meta.get_index_meta(col_names)[0];
    table_meta.indexes.erase(
        std::remove_if(table_meta.indexes.begin(), table_meta.indexes.end(),
                       [&index](const IndexMeta& idx) {
                           return idx == index;  // 根据相等判断删除
                       }),
        table_meta.indexes.end()
    );
    // 持久化
    flush_meta();
    
}