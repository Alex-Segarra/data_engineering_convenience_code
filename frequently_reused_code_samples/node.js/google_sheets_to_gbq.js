
function onOpen(e) {
    // Add a custom menu to the spreadsheet.
  }
  
  /*
  This code shamelessly stolen from: https://www.wouternieuwerth.nl/how-to-push-google-sheets-to-bigquery/
   */
  function test_table_load() {
  
    var projectId = 'PROJECT_ID';
  
    var datasetId = 'DATASET_ID';
  
    var tableId = 'TABLE_IS';
  
    // The URL of the Google Spreadsheet you wish to push to BigQuery:
    var url = 'https://docs.google.com/spreadsheets/d/GOOGLESHEETSLINKHERE/';
      
    // The name of the sheet in the Google Sheets you want to push to BigQuery:
    var sheetName = 'test_sheet';
  
    // Below creates K:V pair to send to BQ with table data.
    var table = {
      tableReference: {
        projectId: projectId,
        datasetId: datasetId,
        tableId: tableId
      },
      // Below is the Schema.  Write in [{name:'colname',type:'data_type'}] format
      schema: {
        fields: [
          {name: 'col1', type: 'STRING'},
          {name: 'col2', type: 'STRING'},
          {name: 'col3', type: 'STRING'},
        ]
      }
    };
    
    // The write disposition tells BigQuery what to do if this table exists
    // WRITE_TRUNCATE: If the table already exists, BigQuery overwrites the table data. 
    // WRITE_APPEND: If the table already exists, BigQuery appends the data to the table.
    // WRITE_EMPTY: If the table already exists and contains data, a 'duplicate' error is returned in the job result.
    var writeDispositionSetting = 'WRITE_TRUNCATE';
    
    //------------------------------------------
    //No edits below this line needed
    
    // Creates a new table if it doesn't exist yet.
    try {BigQuery.Tables.get(projectId, datasetId, tableId)}
    catch (error) {
      table = BigQuery.Tables.insert(table, projectId, datasetId);
      Logger.log('Table created: %s', table.id);
    }
    
    var file = SpreadsheetApp.openByUrl(url).getSheetByName(sheetName);
    // This represents ALL the data
    var rows = file.getDataRange().getValues();
    var rowsCSV = rows.join("\n");
    var blob = Utilities.newBlob(rowsCSV, "text/csv");
    var data = blob.setContentType('application/octet-stream');
  
    // Create the data upload job.
    var job = {
      configuration: {
        load: {
          destinationTable: {
            projectId: projectId,
            datasetId: datasetId,
            tableId: tableId
          },
          skipLeadingRows: 1,
          writeDisposition: writeDispositionSetting
        }
      }
    };
    
    // send the job to BigQuery so it will run your query
    var runJob = BigQuery.Jobs.insert(job, projectId, data);
    Logger.log(runJob.status);
    var jobId = runJob.jobReference.jobId
    Logger.log('jobId: ' + jobId);
    var status = BigQuery.Jobs.get(projectId, jobId);
    
    // wait for the query to finish running before you move on
    while (status.status.state === 'RUNNING') {
      Utilities.sleep(500);
      status = BigQuery.Jobs.get(projectId, jobId);
      Logger.log('Status: ' + status);
    }
    Logger.log('Load Complete!');
  }
  