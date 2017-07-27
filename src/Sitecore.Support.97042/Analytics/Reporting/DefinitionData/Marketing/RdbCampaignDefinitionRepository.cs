namespace Sitecore.Support.Analytics.Reporting.DefinitionData.Marketing
{
    using Sitecore.Analytics.Reporting.DefinitionData.Marketing;
    using System.Collections.Generic;
    using System.Globalization;
    using Sitecore.Analytics.Reporting.DefinitionData.Marketing.RdbCache;
    using Data;
    using Diagnostics;
    using Sitecore.Marketing.Definitions.Campaigns.Data;
    using Configuration;
    using System;
    using System.Data;
    using System.Data.SqlClient;

    /// <summary>
    /// A Campaign definition RDB repository.
    /// </summary>
    public class RdbCampaignDefinitionRepository : RdbDefinitionRepositoryBase<CampaignActivityDefinitionRecord>,
        ICampaignDefinitionRepository
    {
        /// <summary>
        ///   Initializes a new instance of the <see cref="RdbCampaignDefinitionRepository"/> class.
        /// </summary>
        /// <param name="connectionStringName">
        ///   The name of connection string for Reporting database.
        /// </param>
        public RdbCampaignDefinitionRepository([NotNull] string connectionStringName)
            : this(connectionStringName, new DisabledCache<DefinitionKey, CampaignActivityDefinitionRecord>())
        {
            Assert.ArgumentNotNull(connectionStringName, "connectionStringName");
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="RdbCampaignDefinitionRepository"/> class.
        /// </summary>
        /// <param name="connectionStringName">
        /// The name of connection string for Reporting database.
        /// </param>
        /// <param name="cache">
        /// The cache.
        /// </param>
        public RdbCampaignDefinitionRepository([NotNull] string connectionStringName,
            [NotNull] ICache<DefinitionKey, CampaignActivityDefinitionRecord> cache)
            : this(connectionStringName, cache, new DisabledCache<ID, IReadOnlyList<CultureInfo>>())
        {
            Assert.ArgumentNotNull(connectionStringName, "connectionStringName");
            Assert.ArgumentNotNull(cache, "cache");
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="RdbCampaignDefinitionRepository"/> class.
        /// </summary>
        /// <param name="connectionStringName">
        /// The name of connection string for Reporting database.
        /// </param>
        /// <param name="cache">
        /// The cache.
        /// </param>
        /// <param name="availableCulturesCache">
        /// The available cultures cache.
        /// </param>
        public RdbCampaignDefinitionRepository([NotNull] string connectionStringName,
            [NotNull] ICache<DefinitionKey, CampaignActivityDefinitionRecord> cache,
            [NotNull] ICache<ID, IReadOnlyList<CultureInfo>> availableCulturesCache)
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
            return action == StoredProcedureActionType.Save
                ? "Upsert_CampaignActivityDefinitions"
                : action == StoredProcedureActionType.Delete
                    ? "Delete_CampaignActivityDefinitions"
                    : base.ResolveStoredProcedureName(action);
        }

        /// <summary>Resolves the table name.</summary>
        /// <returns>The <see cref="string"/>.</returns>
        protected override string ResolveTableName()
        {
            return "CampaignActivityDefinitions";
        }

        protected override void SaveToRepository(CampaignActivityDefinitionRecord definition, CultureInfo cultureInfo)
        {
            Assert.ArgumentNotNull(definition, "definition");
            Assert.ArgumentNotNull(cultureInfo, "cultureInfo");

            if (cultureInfo.Equals(CultureInfo.InvariantCulture))
                throw new InvalidOperationException("CultureInfo must be different from InvariantCulture.");

            string str = "Upsert_CampaignActivityDefinitions";
            byte[] numArray = Serialize(definition);

            SqlCommand sqlCommand1 = new SqlCommand();
            sqlCommand1.CommandText = str;
            sqlCommand1.CommandType = CommandType.StoredProcedure;

            Factory.GetRetryer().ExecuteNoResult(() =>
            {
               using (SqlCommand sqlCommand = sqlCommand1)
               {
                   sqlCommand.Parameters.AddWithValue("@Id", definition.Id.ToGuid());
                   sqlCommand.Parameters.AddWithValue("@Version", definition.Version);
                   sqlCommand.Parameters.AddWithValue("@Language", cultureInfo.Name);
                   sqlCommand.Parameters.AddWithValue("@IsActive", definition.IsActive);
                   sqlCommand.Parameters.AddWithValue("@Data", numArray);
                   ExecuteTransaction(sqlCommand);
               }
            });
        }
    }
}
