#pragma once

#include <was/storage_account.h>
#include <was/table.h>
#include "AzureUtil.hpp"

using namespace std;
using namespace azure::storage;

class TableEntity :public table_entity {
public:
	string TableName;

	TableEntity(string tableName) {
		this->TableName = tableName;
	}

	TableEntity(string tableName, string partitionKey, string rowKey) {
		this->TableName = tableName;

		wstring wPartitionKey;
		wstring wRowKey;

		AzureUtil::StringToWString(partitionKey, wPartitionKey);
		AzureUtil::StringToWString(rowKey, wRowKey);

		set_partition_key(wPartitionKey);
		set_row_key(wRowKey);
	}

	TableEntity(string tableName, const utility::string_t& partitionKey, const utility::string_t& rowKey)
		:table_entity(partitionKey, rowKey) {
		this->TableName = tableName;
	}

protected:
	void GetStringValue(table_entity::properties_type& properties, wstring key, string& value) {
		try {
			entity_property& entityProp = properties[key];
			if (!entityProp.is_null()) {
				std::wstring wValue = entityProp.string_value();
				AzureUtil::WStringToString(wValue, value);
			}
		}
		catch (const std::exception& e) {
		}
	}

	void GetBinaryValue(table_entity::properties_type& properties, wstring key, std::vector<uint8_t>& value) {
		try {
			entity_property& entityProp = properties[key];
			if (!entityProp.is_null()) {
				value = entityProp.binary_value();
			}
		}
		catch (const std::exception& e) {
		}
	}

	void GetBoolValue(table_entity::properties_type& properties, wstring key, bool& value) {
		try {
			entity_property& entityProp = properties[key];
			if (!entityProp.is_null()) {
				value = entityProp.boolean_value();
			}
		}
		catch (const std::exception& e) {
		}
	}

	void GetDateTimeValue(table_entity::properties_type& properties, wstring key, utility::datetime& value) {
		try {
			entity_property& entityProp = properties[key];
			if (!entityProp.is_null()) {
				value = entityProp.datetime_value();
			}
		}
		catch (const std::exception& e) {
		}
	}

	void GetDoubleValue(table_entity::properties_type& properties, wstring key, double& value) {
		try {
			entity_property& entityProp = properties[key];
			if (!entityProp.is_null()) {
				value = entityProp.double_value();
			}
		}
		catch (const std::exception& e) {
		}
	}

	void GetGuidValue(table_entity::properties_type& properties, wstring key, utility::uuid& value) {
		try {
			entity_property& entityProp = properties[key];
			if (!entityProp.is_null()) {
				value = entityProp.guid_value();
			}
		}
		catch (const std::exception& e) {
		}
	}

	void GetIntValue(table_entity::properties_type& properties, wstring key, int& value) {
		try {
			entity_property& entityProp = properties[key];
			if (!entityProp.is_null()) {
				value = entityProp.int32_value();
			}
		}
		catch (const std::exception& e) {
		}
	}

	void GetLongValue(table_entity::properties_type& properties, wstring key, long& value) {
		try {
			entity_property& entityProp = properties[key];
			if (!entityProp.is_null()) {
				value = entityProp.int64_value();
			}
		}
		catch (const std::exception& e) {
		}
	}

	void GetMapValue(table_entity::properties_type& properties, map<string, string>& mapValue) {
		try {
			for (auto it = properties.begin(); it != properties.end(); it++) {
				string key;
				AzureUtil::WStringToString(it->first, key);
				if (key.find("D_") == 0) {
					string value;
					AzureUtil::WStringToString(it->second.string_value(), value);
					mapValue[key.substr(2)] = value;
				}
			}
		}
		catch (const std::exception& e) {
		}
	}

	void SetStringValue(wstring key, string value) {
		table_entity::properties_type& properties = table_entity::properties();
		std::wstring wValue;
		AzureUtil::StringToWString(value, wValue);
		properties[key] = entity_property(wValue);
	}

	void SetMapValue(map<string, string> mapValue) {
		table_entity::properties_type& properties = table_entity::properties();
		for (auto it = mapValue.begin(); it != mapValue.end(); it++) {
			std::wstring wKey;
			std::wstring wVal;
			AzureUtil::StringToWString("D_" + it->first, wKey);
			AzureUtil::StringToWString(it->second, wVal);
			properties[wKey] = wVal;
		}
	}

	void SetBinaryValue(wstring key, std::vector<uint8_t>& value) {
		table_entity::properties_type& properties = table_entity::properties();
		properties[key] = entity_property(value);
	}

	void SetBoolValue(wstring key, bool value) {
		table_entity::properties_type& properties = table_entity::properties();
		properties[key] = entity_property(value);
	}

	void SetDateTimeValue(wstring key, utility::datetime& value) {
		table_entity::properties_type& properties = table_entity::properties();
		properties[key] = entity_property(value);
	}

	void SetDoubleValue(wstring key, double value) {
		table_entity::properties_type& properties = table_entity::properties();
		properties[key] = entity_property(value);
	}

	void SetGuidValue(wstring key, utility::uuid& value) {
		table_entity::properties_type& properties = table_entity::properties();
		properties[key] = entity_property(value);
	}

	void SetIntValue(wstring key, int value) {
		table_entity::properties_type& properties = table_entity::properties();
		properties[key] = entity_property(value);
	}

	void SetLongValue(wstring key, long value) {
		table_entity::properties_type& properties = table_entity::properties();
		properties[key] = entity_property(value);
	}

	void SetPartitionKey(string partitionKey) {
		wstring wPartitionKey;
		AzureUtil::StringToWString(partitionKey, wPartitionKey);
		set_partition_key(wPartitionKey);
	}

	void SetRowKey(string rowKey) {
		wstring wRowKey;
		AzureUtil::StringToWString(rowKey, wRowKey);
		set_row_key(wRowKey);
	}

public:
	string SetDateTimeAttrUtc0Value(utility::datetime& attr, unsigned long timestamp) {
		string time = AzureUtil::ConvertUnixStampToUTC0Str(timestamp);
		string t = time;
		wstring wtime;
		AzureUtil::StringToWString(time, wtime);
		attr = utility::datetime::from_string(wtime, utility::datetime::date_format::ISO_8601);
		return t;
	}

	void SetDeviceLocalTimeValue(utility::datetime& attr, unsigned long timestamp) {
		string time = AzureUtil::ConvertLocaltsToLocalTimeStr(timestamp);
		wstring wtime;
		AzureUtil::StringToWString(time, wtime);
		attr = utility::datetime::from_string(wtime, utility::datetime::date_format::ISO_8601);
	}
	
	void ClearProperties() {
		table_entity::properties().clear();
	}
};

