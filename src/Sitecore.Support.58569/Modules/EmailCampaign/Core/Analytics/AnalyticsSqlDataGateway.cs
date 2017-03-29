namespace Sitecore.Support.Modules.EmailCampaign.Core.Analytics
{
  using Sitecore;
  using Sitecore.Common;
  using Sitecore.Data;
  using Sitecore.Modules.EmailCampaign.Core.Analytics;
  using System;
  using System.Configuration;
  using System.Data.SqlClient;

  public class AnalyticsSqlDataGateway : Sitecore.Modules.EmailCampaign.Core.Analytics.AnalyticsSqlDataGateway
  {
    protected readonly AnalyticsSqlCommand analyticsSqlCommand;
    protected readonly string ConnectionString;
    private readonly ID intervalFieldId;

    public AnalyticsSqlDataGateway(string batchSize, string commandTimeout) : base(batchSize, commandTimeout)
    {
      this.intervalFieldId = ID.Parse("{07CE07CB-078C-415E-8F92-96690BABD6C7}");
      this.ConnectionString = ConfigurationManager.ConnectionStrings["analytics"].ConnectionString;
      this.analyticsSqlCommand = AnalyticsFactory.Instance.GetAnalyticsSqlCommand(this.ConnectionString, base.SqlCommandTimeout);
    }

    public AnalyticsSqlDataGateway(string batchSize, string commandTimeout, string connectionString, AnalyticsSqlCommand analyticsSqlCommand) : base(batchSize, commandTimeout, connectionString, analyticsSqlCommand)
    {
      this.intervalFieldId = ID.Parse("{07CE07CB-078C-415E-8F92-96690BABD6C7}");
      this.ConnectionString = connectionString;
      this.analyticsSqlCommand = analyticsSqlCommand;
    }

    public override void MarkMessageSent(Guid campaignId, Guid automationStateId, Guid sentStateId, string language)
    {
      string str = Context.ContentDatabase.GetItem(new ID(sentStateId))[this.intervalFieldId];
      if (string.IsNullOrEmpty(str))
      {
        base.MarkMessageSent(campaignId, automationStateId, sentStateId, language);
      }
      else
      {
        int num = int.Parse(str);
        DateTime time = DateTime.UtcNow.AddMinutes((double)num);
        this.analyticsSqlCommand.ExecuteNonQuery("UPDATE AutomationStates SET WakeupDateTime = @wakeupDateTime, StateId = @sentStateId, StateName = @sentStateName, State = 0, Data = @data WHERE AutomationStateId = @automationStateId", new CommandParameter[] { new CommandParameter("automationStateId", automationStateId), new CommandParameter("sentStateId", sentStateId), new CommandParameter("sentStateName", "Send Completed"), new CommandParameter("data", this.FormatLanguage(language)), new CommandParameter("wakeupDateTime", time) });
        this.SetCampaignEndDate(campaignId, DateTime.Now);
      }
    }

    public override void SetAutomationState(Guid automationStateId, Guid automationNextStateId, string automationNextStateName, string fromPossibleStates, bool noWaitFlag)
    {
      string str = Context.ContentDatabase.GetItem(automationNextStateId.ToID())[this.intervalFieldId];
      if (string.IsNullOrEmpty(str))
      {
        base.SetAutomationState(automationStateId, automationNextStateId, automationNextStateName, fromPossibleStates, noWaitFlag);
      }
      else
      {
        int num = int.Parse(str);
        DateTime time = DateTime.UtcNow.AddMinutes((double)num);
        string commandText = noWaitFlag ? "update AutomationStates with(NOWAIT) set StateId = @stateId, WakeupDateTime = @wakeupDateTime, StateName = @stateName, LastAccessedDateTime = @now where AutomationStateId = @automationStateId" : "update AutomationStates set StateId = @stateId, StateName = @stateName, LastAccessedDateTime = @now where AutomationStateId = @automationStateId";
        if (!string.IsNullOrEmpty(fromPossibleStates))
        {
          commandText = commandText + " AND stateId IN (" + fromPossibleStates + ")";
        }
        try
        {
          this.analyticsSqlCommand.ExecuteNonQuery(commandText, new CommandParameter[] { new CommandParameter("automationStateId", automationStateId), new CommandParameter("stateId", automationNextStateId), new CommandParameter("now", DateTime.UtcNow), new CommandParameter("stateName", automationNextStateName), new CommandParameter("wakeupDateTime", time) });
        }
        catch (SqlException exception)
        {
          if (exception.Number != 0x4c6)
          {
            throw;
          }
        }
      }
    }

    private void SetCampaignEndDate(Guid campaignId, DateTime endDate)
    {
      using (SqlConnection connection = new SqlConnection(this.ConnectionString))
      {
        this.SetCampaignEndDate(connection, campaignId, endDate);
      }
    }

    private void SetCampaignEndDate(SqlConnection connection, Guid campaignId, DateTime endDate)
    {
      this.analyticsSqlCommand.ExecuteNonQuery(connection, "UPDATE Campaigns SET EndDate=@endDate WHERE CampaignId=@campaignId", new CommandParameter[] { new CommandParameter("endDate", endDate.ToUniversalTime()), new CommandParameter("campaignId", campaignId) });
    }
  }
}
