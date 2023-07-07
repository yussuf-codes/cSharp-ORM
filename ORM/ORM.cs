using Microsoft.Data.SqlClient;
using System.Collections.Generic;

namespace ORM;
public class ObjectRelationalMapper
{
    private readonly string _connectionString;
    
    public ObjectRelationalMapper(string connectionString)
    {
        _connectionString = connectionString;
    }
    
    private string GetPrimaryKeyName(string table)
    {
        string primaryKeyName = "";
        SqlConnection sqlConnection = new(_connectionString);
        using (sqlConnection)
        {
            string queryString = $"SELECT TOP(1) C.COLUMN_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS T JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE C ON C.CONSTRAINT_NAME=T.CONSTRAINT_NAME WHERE C.TABLE_NAME='{table}' AND T.CONSTRAINT_TYPE='PRIMARY KEY'";
            sqlConnection.Open();
            SqlCommand command = new(queryString, sqlConnection);
            SqlDataReader dataReader = command.ExecuteReader();
            while (dataReader.Read())
            {
                primaryKeyName = dataReader.GetString(0);
            }
            dataReader.Close();
            sqlConnection.Close();
        }
        return primaryKeyName;
    }
    
    public Dictionary<long, Dictionary<string, object>> GetAllRows(string table, long limit = long.MaxValue)
    {
        Dictionary<long, Dictionary<string, object>> dataDictionary = new();
        SqlConnection sqlConnection = new(_connectionString);
        using (sqlConnection)
        {
            string primaryKeyName = GetPrimaryKeyName(table);
            string queryString = $"SELECT TOP ({limit}) * FROM {table};";
            sqlConnection.Open();
            SqlCommand command = new(queryString, sqlConnection);
            SqlDataReader dataReader = command.ExecuteReader();
            int fieldsNumber = dataReader.FieldCount;
            string[] fieldsArray = new string[fieldsNumber];
            for(int i = 0; i < fieldsNumber; i++)
            {
                fieldsArray[i] = dataReader.GetName(i);
            }
            while (dataReader.Read())
            {
                object[] rowValues = new object[fieldsNumber];
                dataReader.GetValues(rowValues);
                Dictionary<string, object> rowDictionary = new();
                for (int i = 0; i < fieldsNumber; i++)
                {
                    rowDictionary.Add(fieldsArray[i], rowValues[i]);
                }
                long primaryKey = (long)dataReader[primaryKeyName];
                dataDictionary.Add(primaryKey, rowDictionary);
            }
            dataReader.Close();
            sqlConnection.Close();
        }
        return dataDictionary;
    }
    
    public Dictionary<string, object> GetRow(string table, long id)
    {
        Dictionary<string, object> rowDictionary = new();
        SqlConnection sqlConnection = new(_connectionString);
        using (sqlConnection)
        {
            string primaryKeyName = GetPrimaryKeyName(table);
            string queryString = $"SELECT * FROM {table} WHERE {primaryKeyName} = {id};";
            sqlConnection.Open();
            SqlCommand command = new(queryString, sqlConnection);
            SqlDataReader dataReader = command.ExecuteReader();
            int fieldsNumber = dataReader.FieldCount;
            string[] fieldsArray = new string[fieldsNumber];
            for(int i = 0; i < fieldsNumber; i++)
            {
                fieldsArray[i] = dataReader.GetName(i);
            }
            while (dataReader.Read())
            {
                object[] rowValues = new object[fieldsNumber];
                dataReader.GetValues(rowValues);
                for (int i = 0; i < fieldsNumber; i++)
                {
                    rowDictionary.Add(fieldsArray[i], rowValues[i]);
                }
            }
            dataReader.Close();
            sqlConnection.Close();
        }
        return rowDictionary;
    }
}