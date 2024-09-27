SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [logging].[PipelineExecution](
	[WorkspaceGuid] [uniqueidentifier] NULL,
	[PipelineRunGuid] [uniqueidentifier] NULL,
	[PipelineParentRunGuid] [uniqueidentifier] NULL,
	[PipelineGuid] [uniqueidentifier] NULL,
	[PipelineName] [varchar](100) NULL,
	[PipelineParameters] [varchar](8000) NULL,
	[TriggerType] [varchar](50) NULL,
	[TriggerGuid] [uniqueidentifier] NULL,
	[TriggerTime] [datetime2](6) NULL,
	[LogType] [varchar](50) NULL,
	[LogDateTime] [datetime2](6) NULL,
	[LogData] [varchar](8000) NULL
) ON [PRIMARY]
GO


