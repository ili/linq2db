﻿using System;

#if NET46
using System.Data.Linq.Mapping;
#else
using System.Data;
#endif

using LinqToDB;
using LinqToDB.Common;

using NUnit.Framework;

namespace Tests.Linq
{
	using Model;

#if NET46
	[System.Data.Linq.Mapping.Table(Name = "Person")]
#else
	[System.ComponentModel.DataAnnotations.Schema.Table("Person")]
#endif
	public class L2SPersons
	{
		private int _personID;

#if NET46
		[System.Data.Linq.Mapping.Column(
			Storage       = "_personID",
			Name          = "PersonID",
			DbType        = "integer(32,0)",
			IsPrimaryKey  = true,
			IsDbGenerated = true,
			AutoSync      = AutoSync.Never,
			CanBeNull     = false)]
#else
		[System.ComponentModel.DataAnnotations.Schema.Column("PersonID",
			TypeName      = "integer(32,0)")]
#endif
		public int PersonID
		{
			get { return _personID;  }
			set { _personID = value; }
		}
#if NET46
		[System.Data.Linq.Mapping.Column]
#else
		[System.ComponentModel.DataAnnotations.Schema.Column]
#endif
		public string FirstName { get; set; } = null!;

#if NET46
		[System.Data.Linq.Mapping.Column]
#else
		[System.ComponentModel.DataAnnotations.Schema.Column]
#endif
		public string LastName = null!;

#if NET46
		[System.Data.Linq.Mapping.Column]
#else
		[System.ComponentModel.DataAnnotations.Schema.Column]
#endif
		public string? MiddleName;

#if NET46
		[System.Data.Linq.Mapping.Column]
#else
		[System.ComponentModel.DataAnnotations.Schema.Column]
#endif
		public string Gender = null!;
	}

	[TestFixture]
	public class L2SAttributeTests : TestBase
	{
		[Test]
		public void IsDbGeneratedTest([IncludeDataSources(TestProvName.AllSQLite)] string context)
		{
			using (var db = GetDataContext(context))
			{
				db.BeginTransaction();

				var id = db.InsertWithIdentity(new L2SPersons
				{
					FirstName = "Test",
					LastName  = "Test",
					Gender    = "M"
				});

				db.GetTable<L2SPersons>().Delete(p => p.PersonID == ConvertTo<int>.From(id));
			}
		}
	}
}
