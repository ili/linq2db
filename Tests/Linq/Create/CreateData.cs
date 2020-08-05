using System;
using System.Data;
using System.IO;
using System.Linq;

using LinqToDB;
using LinqToDB.Data;
using LinqToDB.DataProvider.Access;

using NUnit.Framework;
using Tests;
using Tests.Model;

// for unknown reason order doesn't help on ubuntu 18, so namespace were removed and class name changed to be first in
// sort order
[TestFixture]
[Category(TestCategory.Create)]
[Order(-1)]
// ReSharper disable once InconsistentNaming
// ReSharper disable once TestClassNameSuffixWarning
public class a_CreateData : TestBase
{
	static void RunScript(string configString, string divider, string name, Action<IDbConnection>? action = null, string? database = null)
		=> TestDataConnection.RunScript(
			configString,
			divider,
			name,
			configString == ProviderName.OracleNative || configString == TestProvName.Oracle11Native,
			action,
			database);

	[Test, Order(0)]
	public void CreateDatabase([CreateDatabaseSources] string context)
	{
		switch (context)
		{
			case ProviderName.Firebird                         : RunScript(context,          "COMMIT;", "Firebird", FirebirdAction);       break;
			case TestProvName.Firebird3                        : RunScript(context,          "COMMIT;", "Firebird", FirebirdAction);       break;
			case ProviderName.PostgreSQL                       : RunScript(context,          "\nGO\n",  "PostgreSQL");                     break;
			case ProviderName.PostgreSQL92                     : RunScript(context,          "\nGO\n",  "PostgreSQL");                     break;
			case ProviderName.PostgreSQL93                     : RunScript(context,          "\nGO\n",  "PostgreSQL");                     break;
			case ProviderName.PostgreSQL95                     : RunScript(context,          "\nGO\n",  "PostgreSQL");                     break;
			case TestProvName.PostgreSQL10                     : RunScript(context,          "\nGO\n",  "PostgreSQL");                     break;
			case TestProvName.PostgreSQL11                     : RunScript(context,          "\nGO\n",  "PostgreSQL");                     break;
			case ProviderName.MySql                            : RunScript(context,          "\nGO\n",  "MySql");                          break;
			case ProviderName.MySqlConnector                   : RunScript(context,          "\nGO\n",  "MySql");                          break;
			case TestProvName.MySql55                          : RunScript(context,          "\nGO\n",  "MySql");                          break;
			case TestProvName.MariaDB                          : RunScript(context,          "\nGO\n",  "MySql");                          break;
			case ProviderName.SqlServer2000                    : RunScript(context,          "\nGO\n",  "SqlServer2000");                  break;
			case ProviderName.SqlServer2005                    : RunScript(context,          "\nGO\n",  "SqlServer");                      break;
			case ProviderName.SqlServer2008                    : RunScript(context,          "\nGO\n",  "SqlServer");                      break;
			case ProviderName.SqlServer2012                    : RunScript(context,          "\nGO\n",  "SqlServer");                      break;
			case ProviderName.SqlServer2014                    : RunScript(context,          "\nGO\n",  "SqlServer");                      break;
			case ProviderName.SqlServer2017                    : RunScript(context,          "\nGO\n",  "SqlServer");                      break;
			case TestProvName.SqlAzure                         : RunScript(context,          "\nGO\n",  "SqlServer");                      break;
			case ProviderName.SQLiteMS                         : RunScript(context,          "\nGO\n",  "SQLite",   SQLiteAction);
			                                                     RunScript(context+ ".Data", "\nGO\n",  "SQLite",   SQLiteAction);         break;
			case ProviderName.OracleManaged                    : RunScript(context,          "\n/\n",   "Oracle");                         break;
			case TestProvName.Oracle11Managed                  : RunScript(context,          "\n/\n",   "Oracle");                         break;
			case ProviderName.SybaseManaged                    : RunScript(context,          "\nGO\n",  "Sybase",   null, "TestDataCore"); break;
			case ProviderName.SQLiteClassic                    : RunScript(context,          "\nGO\n",  "SQLite",   SQLiteAction);
			                                                     RunScript(context+ ".Data", "\nGO\n",  "SQLite",   SQLiteAction);         break;
			case TestProvName.SQLiteClassicMiniProfilerMapped  : RunScript(context,          "\nGO\n",  "SQLite",   SQLiteAction);         break;
			case TestProvName.SQLiteClassicMiniProfilerUnmapped: RunScript(context,          "\nGO\n",  "SQLite",   SQLiteAction);         break;
			case ProviderName.Informix                         : RunScript(context,          "\nGO\n",  "Informix", InformixAction);       break;
			case ProviderName.InformixDB2                      : RunScript(context,          "\nGO\n",  "Informix", InformixDB2Action);    break;
			case ProviderName.DB2                              : RunScript(context,          "\nGO\n",  "DB2");                            break;
			case ProviderName.SapHanaNative                    : RunScript(context,          ";;\n"  ,  "SapHana");                        break;
			case ProviderName.SapHanaOdbc                      : RunScript(context,          ";;\n"  ,  "SapHana");                        break;
			case ProviderName.Access                           : RunScript(context,          "\nGO\n",  "Access",   AccessAction);
			                                                     RunScript(context+ ".Data", "\nGO\n",  "Access",   AccessAction);         break;
			case ProviderName.AccessOdbc                       : RunScript(context,          "\nGO\n",  "Access",   AccessODBCAction);
			                                                     RunScript(context+ ".Data", "\nGO\n",  "Access",   AccessODBCAction);     break;
			case ProviderName.SqlCe                            : RunScript(context,          "\nGO\n",  "SqlCe");
			                                                     RunScript(context+ ".Data", "\nGO\n",  "SqlCe");                          break;
#if NET46
			case ProviderName.Sybase                           : RunScript(context,          "\nGO\n",  "Sybase",   null, "TestData");     break;
			case ProviderName.OracleNative                     : RunScript(context,          "\n/\n",   "Oracle");                         break;
			case TestProvName.Oracle11Native                   : RunScript(context,          "\n/\n",   "Oracle");                         break;
#endif
			default                                            : throw new InvalidOperationException(context);
		}
	}

	static void AccessODBCAction(IDbConnection connection)
	{

		using (var conn = AccessTools.CreateDataConnection(connection, ProviderName.AccessOdbc))
		{
			conn.Execute(@"
				INSERT INTO AllTypes
				(
					bitDataType, decimalDataType, smallintDataType, intDataType,tinyintDataType, moneyDataType, floatDataType, realDataType,
					datetimeDataType,
					charDataType, varcharDataType, textDataType, ncharDataType, nvarcharDataType, ntextDataType,
					binaryDataType, varbinaryDataType, imageDataType, oleobjectDataType,
					uniqueidentifierDataType
				)
				VALUES
				(
					1, 2222222, 25555, 7777777, 100, 100000, 20.31, 16.2,
					?,
					'1', '234', '567', '23233', '3323', '111',
					?, ?, ?, ?,
					?
				)",
				new
				{
					datetimeDataType         = new DateTime(2012, 12, 12, 12, 12, 12),

					binaryDataType           = new byte[] { 1, 2, 3, 4 },
					varbinaryDataType        = new byte[] { 1, 2, 3, 5 },
					imageDataType            = new byte[] { 3, 4, 5, 6 },
					oleobjectDataType        = new byte[] { 5, 6, 7, 8 },

					uniqueidentifierDataType = new Guid("{6F9619FF-8B86-D011-B42D-00C04FC964FF}"),
				});
		}
	}

	static void AccessAction(IDbConnection connection)
	{
		using (var conn = AccessTools.CreateDataConnection(connection, ProviderName.Access))
		{
			conn.Execute(@"
				INSERT INTO AllTypes
				(
					bitDataType, decimalDataType, smallintDataType, intDataType,tinyintDataType, moneyDataType, floatDataType, realDataType,
					datetimeDataType,
					charDataType, varcharDataType, textDataType, ncharDataType, nvarcharDataType, ntextDataType,
					binaryDataType, varbinaryDataType, imageDataType, oleobjectDataType,
					uniqueidentifierDataType
				)
				VALUES
				(
					1, 2222222, 25555, 7777777, 100, 100000, 20.31, 16.2,
					@datetimeDataType,
					'1', '234', '567', '23233', '3323', '111',
					@binaryDataType, @varbinaryDataType, @imageDataType, @oleobjectDataType,
					@uniqueidentifierDataType
				)",
				new
				{
					datetimeDataType = new DateTime(2012, 12, 12, 12, 12, 12),

					binaryDataType    = new byte[] { 1, 2, 3, 4 },
					varbinaryDataType = new byte[] { 1, 2, 3, 5 },
					imageDataType     = new byte[] { 3, 4, 5, 6 },
					oleobjectDataType = new byte[] { 5, 6, 7, 8 },

					uniqueidentifierDataType = new Guid("{6F9619FF-8B86-D011-B42D-00C04FC964FF}"),
				});
		}
	}

	void FirebirdAction(IDbConnection connection)
	{
		using (var conn = LinqToDB.DataProvider.Firebird.FirebirdTools.CreateDataConnection(connection))
		{
			conn.Execute(@"
				UPDATE PERSON
				SET
					FIRSTNAME = @FIRSTNAME,
					LASTNAME  = @LASTNAME
				WHERE PERSONID = 4",
				new
				{
					FIRSTNAME = "Jürgen",
					LASTNAME  = "König",
				});
		}
	}

	static void SQLiteAction(IDbConnection connection)
	{
		using (var conn = LinqToDB.DataProvider.SQLite.SQLiteTools.CreateDataConnection(connection))
		{
			conn.Execute(@"
				UPDATE AllTypes
				SET
					binaryDataType           = @binaryDataType,
					varbinaryDataType        = @varbinaryDataType,
					imageDataType            = @imageDataType,
					uniqueidentifierDataType = @uniqueidentifierDataType
				WHERE ID = 2",
				new
				{
					binaryDataType           = new byte[] { 1 },
					varbinaryDataType        = new byte[] { 2 },
					imageDataType            = new byte[] { 0, 0, 0, 3 },
					uniqueidentifierDataType = new Guid("{6F9619FF-8B86-D011-B42D-00C04FC964FF}"),
				});
		}
	}

	static void InformixAction(IDbConnection connection)
	{
		using (var conn = LinqToDB.DataProvider.Informix.InformixTools.CreateDataConnection(connection, ProviderName.Informix))
		{
			conn.Execute(@"
				UPDATE AllTypes
				SET
					byteDataType = ?,
					textDataType = ?
				WHERE ID = 2",
				new
				{
					blob = new byte[] { 1, 2 },
					text = "BBBBB"
				});
		}
	}

	static void InformixDB2Action(IDbConnection connection)
	{
		using (var conn = LinqToDB.DataProvider.Informix.InformixTools.CreateDataConnection(connection, ProviderName.InformixDB2))
		{
			conn.Execute(@"
				UPDATE AllTypes
				SET
					byteDataType = ?,
					textDataType = ?
				WHERE ID = 2",
				new
				{
					blob = new byte[] { 1, 2 },
					text = "BBBBB"
				});
		}
	}
}
