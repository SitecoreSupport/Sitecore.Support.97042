namespace Sitecore.Support.Analytics.Reporting.DefinitionData.Marketing
{
    using Sitecore.Analytics.Reporting.DefinitionData.Marketing;
    using Configuration;
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using Sitecore.Analytics.Reporting.DefinitionData.Marketing.RdbCache;
    using Data;
    using Diagnostics;
    using Sitecore.Marketing.Definitions.Goals.Data;
    using System.Data;
    using System.Data.SqlClient;

    /// <summary>The goal definition reporting database repository.</summary>
    public class GoalDefinitionRdbRepository : RdbDefinitionRepositoryBase<GoalDefinitionRecord>, IGoalDefinitionRepository
    {
        /// <summary>Initializes a new instance of the <see cref="GoalDefinitionRdbRepository"/> class.</summary>
        /// <param name="connectionStringName">Name of the connection string.</param>
        public GoalDefinitionRdbRepository([NotNull] string connectionStringName) : this(connectionStringName, new DisabledCache<DefinitionKey, GoalDefinitionRecord>())
        {
            Assert.ArgumentNotNull(connectionStringName, "connectionStringName");
        }

        /// <summary>Initializes a new instance of the <see cref="GoalDefinitionRdbRepository"/> class.</summary>
        /// <param name="connectionStringName">Name of the connection string.</param>
        /// <param name="cache">The cache.</param>
        public GoalDefinitionRdbRepository([NotNull] string connectionStringName, [NotNull]ICache<DefinitionKey, GoalDefinitionRecord> cache)
          : this(connectionStringName, cache, new DisabledCache<ID, IReadOnlyList<CultureInfo>>())
        {
            Assert.ArgumentNotNull(connectionStringName, "connectionStringName");
            Assert.ArgumentNotNull(cache, "cache");
        }

        /// <summary>Initializes a new instance of the <see cref="GoalDefinitionRdbRepository"/> class.</summary>
        /// <param name="connectionStringName">Name of the connection string.</param>
        /// <param name="cache">The cache.</param>
        /// <param name="availableCulturesCache">The available cultures cache.</param>
        public GoalDefinitionRdbRepository([NotNull] string connectionStringName, [NotNull]ICache<DefinitionKey, GoalDefinitionRecord> cache, [NotNull]ICache<ID, IReadOnlyList<CultureInfo>> availableCulturesCache)
          : base(connectionStringName, cache, availableCulturesCache)
        {
            Assert.ArgumentNotNull(connectionStringName, "connectionStringName");
            Assert.ArgumentNotNull(cache, "cache");
            Assert.ArgumentNotNull(availableCulturesCache, "availableCulturesCache");
        }

        /// <summary>Resolves the stored procedure name.</summary>
        /// <param name="action">The action.</param>
        /// <returns>The <see cref="string"/>.</returns>
        protected override string ResolveStoredProcedureName(StoredProcedureActionType action)
        {
            return action == StoredProcedureActionType.Save ? "Upsert_GoalDefinition" :
                   action == StoredProcedureActionType.Delete ? "Delete_GoalDefinitions" :
                   base.ResolveStoredProcedureName(action);
        }

        /// <summary>Resolves the table name.</summary>
        /// <returns>The <see cref="string"/>.</returns>
        protected override string ResolveTableName()
        {
            return "GoalDefinitions";
        }

        protected override void SaveToRepository(GoalDefinitionRecord definition, CultureInfo cultureInfo)
        {
            Assert.ArgumentNotNull(definition, "definition");
            Assert.ArgumentNotNull(cultureInfo, "cultureInfo");

            if (cultureInfo.Equals(CultureInfo.InvariantCulture))
                throw new InvalidOperationException("CultureInfo must be different from InvariantCulture.");

            string str = "Upsert_GoalDefinition";
            byte[] buffer = Serialize(definition);

            SqlCommand sqlCommand1 = new SqlCommand();
            sqlCommand1.CommandText = str;
            sqlCommand1.CommandType = CommandType.StoredProcedure;
            SqlCommand command2 = sqlCommand1;

            Factory.GetRetryer().ExecuteNoResult(() =>
            {
                using (SqlCommand sqlCommand2 = command2)
                {
                    sqlCommand2.Parameters.AddWithValue("@Id", definition.Id.ToGuid());
                    sqlCommand2.Parameters.AddWithValue("@Version", definition.Version);
                    sqlCommand2.Parameters.AddWithValue("@Language", cultureInfo.Name);
                    sqlCommand2.Parameters.AddWithValue("@IsActive", definition.IsActive);
                    sqlCommand2.Parameters.AddWithValue("@Data", buffer);
                    ExecuteTransaction(sqlCommand2);
                }
            });
        }
    }
}

