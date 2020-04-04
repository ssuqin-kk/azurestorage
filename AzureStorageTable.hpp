#pragma once
#include <was/storage_account.h>
#include <was/table.h>
#include <map>
#include "AzureUtil.hpp"
#include "TableEntity.hpp"

typedef std::string String;

/// doc:https://docs.azure.cn/zh-cn/cosmos-db/table-storage-how-to-use-c-plus

template<typename T>
class AzureStorageTable {
private:
	AzureStorageTable() {
	}
	
	AzureStorageTable(const AzureStorageTable&) {
	}
	
	AzureStorageTable& operator=(const AzureStorageTable&) {
	}

	azure::storage::cloud_table_client TableClient;
	std::string LastError = "";
	map<String, azure::storage::cloud_table> Tables;

public:

	std::string GetStorageConnectionString(String storageCount, String storageAccountKey, String protocol = "https") {
		String storageConnectionString;
		storageConnectionString.append("DefaultEndpointsProtocol=");
		storageConnectionString.append(protocol);
		storageConnectionString.append(";AccountName=");
		storageConnectionString.append(storageCount);
		storageConnectionString.append(";AccountKey=");
		storageConnectionString.append(storageAccountKey);
		return storageConnectionString;
	}

	static AzureStorageTable<T>& GetInstance() {
		static AzureStorageTable<T> instance;
		return instance;
	}

	bool Initial(string connStr) {

		try {
			if (connStr.empty()) {
				return false;
			}

			std::wstring wStorageConnectionString;
			AzureUtil::StringToWString(connStr, wStorageConnectionString);

			// Retrieve the storage account from the connection string.
			azure::storage::cloud_storage_account storage_account = azure::storage::cloud_storage_account::parse(wStorageConnectionString);
			// Create the table client.
			TableClient = storage_account.create_cloud_table_client();
		}
		catch (const std::exception& e) {
			LastError = e.what();
			return false;
		}
		return true;
	}

	bool GetTable(String tableName) {
		try {
			if (Tables.find(tableName) == Tables.end()) {
				std::wstring wTableName;
				AzureUtil::StringToWString(tableName, wTableName);

				cloud_table table = TableClient.get_table_reference(wTableName);
				table.create_if_not_exists();
				Tables[tableName] = table;
			}
		}
		catch (const std::exception& e) {
			LastError = e.what();
			return false;
		}
		return true;
	}


	bool Add(T& entity, table_result& result) {
		
		if (entity.TableName.empty() || entity.properties().size() <= 0) {
			std::cout << "insert1 fail,entity.TableName:(" << entity.TableName << ")," << "entity.properties().size:("
				<< entity.properties().size() << ")";
			return false;
		}
		
		if (!GetTable(entity.TableName)) {
			return false;
		}

		try {
			// Create the table operation that inserts the customer entity.
			azure::storage::table_operation insert_operation = azure::storage::table_operation::insert_entity(entity);
			// Execute the insert operation.
			result = Tables[entity.TableName].execute(insert_operation);
		} catch (const std::exception& e) {
			LastError = e.what();
			return false;
		}

		return true;
	}

	bool BatchAdd(std::vector<T>& entities, vector<table_result>& results) {

		if (entities.size() <= 0 || entities.front().TableName.empty() ||
			entities.front().properties().size() <= 0) {
			std::cout << "batch insert fail, entities.front().TableName:(" << entities.front().TableName << ")," << "entities.front().properties().size:("
				<< entities.front().properties().size() << ")";
			return false;
		}

		string tableName = entities.front().TableName;

		if (!GetTable(tableName)) {
			return false;
		}

		try {
			// Define a batch operation.
			azure::storage::table_batch_operation batch_operation;
		
			for (auto entity:entities) {
				// Add entity to the batch insert operation.
				batch_operation.insert_or_replace_entity(entity);
			}

			// Execute the batch operation.
			results = Tables[tableName].execute_batch(batch_operation);
		}
		catch (const std::exception& e) {
			LastError = e.what();
			return false;
		}
		return true;
	}

	bool AddOrUpdate(T& entity, table_result& result) {

		if (entity.TableName.empty() || entity.properties().size() <= 0) {
			std::cout << "add or update fail!,entity.TableName:(" << entity.TableName << "),entity.properties().size:("
				<< entity.properties().size() << ")";
			return false;
		}

		if (!GetTable(entity.TableName)) {
			return false;
		}

		try {
			table_operation insert_or_replace_operation = table_operation::insert_or_replace_entity(entity);
			result = Tables[entity.TableName].execute(insert_or_replace_operation);
		}
		catch (const std::exception& e) {
			LastError = e.what();
			return false;
		}
		return true;
	}

	bool Delete(T& entity, table_result& result) {

		if (entity.TableName.empty()) {
			std::cout << "delete fail, entity.TableName is empty!";
			return false;
		}

		if (!GetTable(entity.TableName)) {
			return false;
		}

		try {
			// Create an operation to retrieve the entity with partition key of "Smith" and row key of "Jeff".
			table_operation retrieve_operation = table_operation::retrieve_entity(entity.partition_key(), entity.row_key());
			table_result retrieve_result = Tables[entity.TableName].execute(retrieve_operation);
			table_operation delete_operation = table_operation::delete_entity(retrieve_result.entity());
			result = Tables[entity.TableName].execute(delete_operation);
		}
		catch (const std::exception& e) {
			LastError = e.what();
			return false;
		}

		return true;
	}


	bool Update(T& entity, table_result& result) {

		if (entity.TableName.empty() || entity.properties().size() <= 0) {
			std::cout << "update fail, entity.TableName:(" << entity.TableName << "),entity.properties().size:("
				<< entity.properties().size() << ")";
			return false;
		}

		if (!GetTable(entity.TableName)) {
			return false;
		}
		
		try {
			table_operation replace_operation = table_operation::replace_entity(entity);
			result = Tables[entity.TableName].execute(replace_operation);
		}
		catch (const std::exception& e) {
			LastError = e.what();
			return false;
		}

		return true;
	}

	bool Query(T& entity, utility::string_t& conditionSql, vector<T>& results) {

		if (entity.TableName.empty()) {
			std::cout << "query fail,table_name:(" << entity.TableName << ")";
			return false;
		}

		if (!GetTable(entity.TableName)) {
			return false;
		}

		try {
			azure::storage::table_query query;
			query.set_filter_string(conditionSql);
			table_query_iterator it = Tables[entity.TableName].execute_query(query);
			azure::storage::table_query_iterator end_of_results;
			for (; it != end_of_results; ++it) {
				T t(it->partition_key(), it->row_key());
				t.Assign(it->properties());
				results.push_back(t);
			}
		}
		catch (const std::exception& e) {
			LastError = e.what();
			return false;
		}

		return true;
	}


	bool Query(T& entity) {

		if (entity.TableName.empty()) {
			std::cout << "query fail, model.TableName is empty!";
			return false;
		}

		try {
			if (!GetTable(entity.TableName)) {
				return false;
			}

			table_operation retrieve_operation = table_operation::retrieve_entity(entity.partition_key(), entity.row_key());
			table_result result = Tables[entity.TableName].execute(retrieve_operation);
			entity.Assign((table_entity::properties_type&)result.entity().properties());
		}
		catch (const std::exception& e) {
			LastError = e.what();
			return false;
		}

		return true;
	}

	bool QueryPropertiesCollection(T& entity, std::vector<string>& queryColumns, 
		vector<table_entity::properties_type>& propertiesCollection) {

		if (entity.TableName.empty() || queryColumns.size() <= 0) {
			std::cout << "query properties collection fail, table_name:(" << entity.TableName << ")," <<
				"queryColumns.size:(" << queryColumns.size() << ")";
			return false;
		}

		if (!GetTable(entity.TableName)) {
			return false;
		}

		try {
			azure::storage::table_query query;
			vector<wstring> selectColumns;

			for (auto columnName : queryColumns) {
				std::wstring wColumnName;
				AzureUtil::StringToWString(columnName, wColumnName);
				selectColumns.push_back(wColumnName);
			}
			query.set_select_columns(selectColumns);

			azure::storage::table_query_iterator it = Tables[entity.TableName].execute_query(query);
			azure::storage::table_query_iterator end_of_results;
			for (; it != end_of_results; ++it) {
				propertiesCollection.push_back(it->properties());
			}
		}
		catch (const std::exception& e) {
			LastError = e.what();
			return false;
		}

		return true;
	}


	bool DeleteTable(utility::string_t wTableName) {

		if (wTableName.empty()) {
			std::cout << "delete table fail, table_name is empty!";
			return false;
		}

		try {
			cloud_table table = TableClient.get_table_reference(wTableName);
			table.delete_table_if_exists();

			string tableName;
			AzureUtil::WStringToString(wTableName, tableName);

			for (auto it = Tables.begin(); it != Tables.end(); it++) {
				if (it->first == tableName) {
					Tables.erase(it);
					break;
				}
			}
		}
		catch (const std::exception& e) {
			LastError = e.what();
			return false;
		}

		return true;
	}

	void ClearTableCache() {
		Tables.clear();
	}

	string GetLastError() {
		return AzureUtil::AsciiCharPtrToUtf8(LastError.c_str());
	}
};