/****** Object:  StoredProcedure [logging].[sp_AuditPipeline]    Script Date: 26/01/2024 15:26:02 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


/**********************************************************************************************************
* Purpose:		This stored procedure logs Events to the [logging].[PipelineExecution]
*              
*              
* Revision Date/Time:
*  2024-01-01		Erwin de Kreuk (InSpark) - Initial creation of SP
*
*
**********************************************************************************************************/

Create PROCEDURE [logging].[sp_AuditPipeline]
	 @PipelineGuid UNIQUEIDENTIFIER				= NULL
    ,@PipelineName VARCHAR(100)				    = NULL
    ,@PipelineRunGuid UNIQUEIDENTIFIER			= NULL
    ,@PipelineParentRunGuid UNIQUEIDENTIFIER	= NULL
    ,@PipelineParameters VARCHAR(8000)  	    = NULL
    ,@TriggerType VARCHAR(50)					= NULL
    ,@TriggerGuid UNIQUEIDENTIFIER				= NULL
    ,@TriggerTime datetime						= NULL
    ,@LogData VARCHAR(8000)                     = NULL
	,@LogType	varchar(50)						--Choice between Start/End/Fail, based on this Type the correct execution will be done
	,@WorkspaceGuid UNIQUEIDENTIFIER		    = NULL

AS

    INSERT INTO [logging].[PipelineExecution]
           ([PipelineRunGuid]
           ,[PipelineParentRunGuid]
           ,[PipelineGuid]
           ,[PipelineName]
           ,[PipelineParameters]
           ,[TriggerType]
           ,[TriggerGuid]
           ,[TriggerTime]
           ,[LogDateTime]
           ,[LogType]
           ,[LogData]
           ,[WorkspaceGuid]

			)
     VALUES (
           @PipelineRunGuid,
           @PipelineParentRunGuid,
           @PipelineGuid,
           @PipelineName,
           @PipelineParameters,
           @TriggerType,
           @TriggerGuid,
           @TriggerTime,
           getdate(),
           @LogType,
           @LogData,
           @WorkspaceGuid

           )




GO


