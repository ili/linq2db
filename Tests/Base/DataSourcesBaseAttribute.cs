using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

using NUnit.Framework;
using NUnit.Framework.Interfaces;

namespace Tests
{
	public abstract class DataSourcesBaseAttribute : NUnitAttribute, IParameterDataSource
	{
		public bool     IncludeLinqService { get; }
		public string[] Providers          { get; }

		public static bool NoLinqService   { get; set; }

		protected DataSourcesBaseAttribute(bool includeLinqService, string[] providers)
		{
			IncludeLinqService = includeLinqService;
			Providers          = providers.SelectMany(p => p.Split(',').Select(_ => _.Trim())).ToArray();
		}

		public IEnumerable GetData(IParameterInfo parameter)
		{
			var skipAttrs = new HashSet<string>(
				from a in parameter.Method.GetCustomAttributes<SkipCategoryAttribute>(true)
				where a.ProviderName != null && TestBase.SkipCategories.Contains(a.Category)
				select a.ProviderName);

			var methodName     = parameter.Method.Name;
			var typeMethodName = $"{parameter.Method.MethodInfo.DeclaringType.Name}.{methodName}";
			var fullMethodName = $"{parameter.Method.MethodInfo.DeclaringType.FullName}.{methodName}";
			var skipTests      = TestBase.SkipTests;

			var providers = skipAttrs.Count == 0 
				? GetProviders() 
				: GetProviders().Where(a => !skipAttrs.Contains(a));

			providers = providers.Where(_ => !skipTests[_].Contains(methodName)
				&& !skipTests[_].Contains(typeMethodName)
				&& !skipTests[_].Contains(fullMethodName));

			if (NoLinqService || !IncludeLinqService)
				return providers;

			return providers.Concat(providers.Select(p => p + ".LinqService"));
		}

		protected abstract IEnumerable<string> GetProviders();
	}
}
