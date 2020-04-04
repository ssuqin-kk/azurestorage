#pragma once

#include <was/storage_account.h>
#include <was/table.h>
#include<vector>
#include "AzureUtil.hpp"

using namespace azure::storage;
using namespace std;

class QueryLocalOperator {
public:
	static std::wstring OpAnd() {
		return query_logical_operator::op_and;
	}

	static std::wstring OpNot() {
		return query_logical_operator::op_not;
	}

	static std::wstring OpOr() {
		return query_logical_operator::op_or;
	}

	static std::wstring OpNone() {
		return L"none";
	}
};

class ComparisonOperator {
public:

	static utility::string_t Equal() {
		return query_comparison_operator::equal;
	}

	static utility::string_t NotEqualSql(){
		return query_comparison_operator::not_equal;
	}

	/// <summary>
	/// Represents the Greater Than operator.
	/// </summary>
	static utility::string_t GreaterThan() {
		return query_comparison_operator::greater_than;
	}

	/// <summary>
	/// Represents the Greater Than or Equal operator.
	/// </summary>
	static utility::string_t GreaterThanOrEqual() {
		return query_comparison_operator::greater_than_or_equal;
	}

	/// <summary>
	/// Represents the Less Than operator.
	/// </summary>
	static utility::string_t LessThan() {
		return query_comparison_operator::less_than;
	}

	/// <summary>
	/// Represents the Less Than or Equal operator.
	/// </summary>
	static utility::string_t LessThanOrEqualSql() {
		return query_comparison_operator::less_than_or_equal;
	}
};

class SqlParameter {

public:
	utility::string_t Sql;

	SqlParameter() {
	}

	SqlParameter(const string propertyName, const const utility::string_t& comparisonOperator,
		const string value) {
		std::wstring wPropertyName;
		std::wstring wValue;

		AzureUtil::StringToWString(propertyName, wPropertyName);
		AzureUtil::StringToWString(value, wValue);

		Sql = table_query::generate_filter_condition(wPropertyName, comparisonOperator, wValue);
	}

	SqlParameter(const string propertyName, const const utility::string_t& comparisonOperator,
		bool value) {
		std::wstring wPropertyName;
		AzureUtil::StringToWString(propertyName, wPropertyName);

		Sql = table_query::generate_filter_condition(wPropertyName, comparisonOperator, value);
	}

	SqlParameter(const string propertyName, const const utility::string_t& comparisonOperator,
		utility::datetime value) {
		std::wstring wPropertyName;
		AzureUtil::StringToWString(propertyName, wPropertyName);

		Sql = table_query::generate_filter_condition(wPropertyName, comparisonOperator, value);
	}

	SqlParameter(const string propertyName, const const utility::string_t& comparisonOperator,
		double value) {
		std::wstring wPropertyName;
		AzureUtil::StringToWString(propertyName, wPropertyName);

		Sql = table_query::generate_filter_condition(wPropertyName, comparisonOperator, value);
	}

	SqlParameter(const string propertyName, const const utility::string_t& comparisonOperator,
		const utility::uuid& value) {
		std::wstring wPropertyName;
		AzureUtil::StringToWString(propertyName, wPropertyName);

		Sql = table_query::generate_filter_condition(wPropertyName, comparisonOperator, value);
	}

	SqlParameter(const string propertyName, const const utility::string_t& comparisonOperator,
		int value) {
		std::wstring wPropertyName;
		AzureUtil::StringToWString(propertyName, wPropertyName);

		Sql = table_query::generate_filter_condition(wPropertyName, comparisonOperator, value);
	}

	SqlParameter(const string propertyName, const const utility::string_t& comparisonOperator,
		long value) {
		std::wstring wPropertyName;
		AzureUtil::StringToWString(propertyName, wPropertyName);

		Sql = table_query::generate_filter_condition(wPropertyName, comparisonOperator, value);
	}

	utility::string_t& GetSql() {
		return Sql;
	}
};

class Condition {
private:
	vector<Condition> Child;

public:
	utility::string_t LogicalOperator;
	SqlParameter Param;
	utility::string_t Sql;

	Condition(string sql, const utility::string_t& logicalOperator) {
		this->LogicalOperator = logicalOperator;
		AzureUtil::StringToWString(sql, Sql);
	}

	Condition(SqlParameter param, const utility::string_t& logicalOperator) {
		this->LogicalOperator = logicalOperator;
		this->Param = param;
	}

	utility::string_t& GetSql() {
		if (!Sql.empty()) {
			return Sql;
		}
		return Param.GetSql();
	}
};


class SqlWhere {

private:
	vector<Condition> Conditions;

public:
	void Add(const utility::string_t& logicalOperator, const string sql) {
		Conditions.push_back(Condition(sql, logicalOperator));
	}

	void Add(const utility::string_t& logicalOperator, const string propertyName,
		const utility::string_t& comparisonOperator, const string value) {
		Conditions.push_back(Condition(SqlParameter(propertyName, comparisonOperator, value),
			logicalOperator));
	}

	void Add(const utility::string_t& logicalOperator, const string propertyName,
		const utility::string_t& comparisonOperator, bool value) {
		Conditions.push_back(Condition(SqlParameter(propertyName, comparisonOperator, value),
			logicalOperator));
	}

	void Add(const utility::string_t& logicalOperator, const string propertyName,
		const utility::string_t& comparisonOperator, utility::datetime value) {
		Conditions.push_back(Condition(SqlParameter(propertyName, comparisonOperator, value),
			logicalOperator));
	}

	void Add(const utility::string_t& logicalOperator, const string propertyName,
		const utility::string_t& comparisonOperator, double value) {
		Conditions.push_back(Condition(SqlParameter(propertyName, comparisonOperator, value),
			logicalOperator));
	}

	void Add(const utility::string_t& logicalOperator, const string propertyName,
		const utility::string_t& comparisonOperator, const utility::uuid& value) {
		Conditions.push_back(Condition(SqlParameter(propertyName, comparisonOperator, value),
			logicalOperator));
	}

	void Add(const utility::string_t& logicalOperator, const string propertyName,
		const utility::string_t& comparisonOperator, int value) {
		Conditions.push_back(Condition(SqlParameter(propertyName, comparisonOperator, value),
			logicalOperator));
	}

	void Add(const utility::string_t& logicalOperator, const string propertyName,
		const utility::string_t& comparisonOperator, long value) {
		Conditions.push_back(Condition(SqlParameter(propertyName, comparisonOperator, value),
			logicalOperator));
	}

	utility::string_t GetSql() {
		utility::string_t sql;
		for (auto it = Conditions.begin(); it != Conditions.end(); it++) {
			if (it->LogicalOperator == QueryLocalOperator::OpNone()) {
				sql = it->GetSql();
			}
			else {
				table_query::combine_filter_conditions(sql, it->LogicalOperator, it->GetSql());
			}
		}
		return sql;
	}
};