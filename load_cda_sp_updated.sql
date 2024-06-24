	create or replace procedure load_cda_sp(module string, table_prefix string, batch_size FLOAT)
	returns string
	language javascript
    execute as caller
	as
	$$
    
    function log(log_tbl_nm ,run_id , task_nm , mdl_ld_sts , load_ts ,tbl_name, insert_results ){
    
		var query;
		var statement;
		var rs;
	
		if (insert_results = '1'){
			
			query = 'insert into identifier(:1) select :2, :3,to_timestamp_ltz(\''+load_ts +'\'), current_timestamp(), \'INFO\', :4, :5, last_query_id(), parse_json(concat(\'[\',listagg(to_json(object_construct(*)), \',\'),\']\')) from table(result_scan(last_query_id()));';
			
			statement = snowflake.createStatement( 
						{sqlText: query,
							binds: [log_tbl_nm,run_id, task_nm, tbl_name, mdl_ld_sts].map(function(x){return x === undefined ? null : x})
						} );  
		}
		else{
			query = 'insert into identifier(:1) select :2, :3, to_timestamp_ltz(\''+load_ts +'\'), current_timestamp(), \'INFO\', null, :4, null, null';

			statement = snowflake.createStatement( 
						{sqlText: query,
						binds: [log_tbl_nm,run_id, task_nm, mdl_ld_sts] 
						} );  
		}
			rs = statement.execute();
			
		return 'success';
    }

	  // !!! Prerequisite: Presence of LOAD_CDA schema and LOAD_CDA.CDA_LOAD_LOG table !!! ---
		//--module varchar = 'billingcenter';
		//--table_prefix =  'BCTL_AGENCYWRITEOFFTYPE'; //-- Uncomment the Test source query for testing
		//--batch_size int = 1;
		
		var db_name  = 'APISERO';
		var common_load_schema  = db_name + '.' + 'load_cda';
		var load_ts  =  new Date().getTime();

		var rs_task ;
		var rs_task1;
		var task_name;
		var run_id;

		var proc_start_ts ;
		var proc_end_ts ;
		var module_load_schema ;
        var module_stage_schema ;
		var module_warehouse_schema ;
		var module_tbl_count  = 0;
		var rows_parsed  = 0;
		var rows_loaded  = 0;
		var errors_seen  = 0;
		var tbl_name ;
		var tbl_rows_parsed ;
		var tbl_rows_loaded ;
		var tbl_errors_seen ;
		var tbl_proc_start_ts ;
		var tbl_proc_end_ts ;
		var module_load_status ;
		var query ;
		var rs_tables ;
		var rs_infer_schema ;
		var rs_copy_result ;
		var rs_folder_list ;
		var folder_list  = '';
		var create_view_query ;
		var insert_savepoint_query ;
		var folder_list_query ;
		var dw_merge_query ;
		var log_tbl_name ;
		var log_detail_tbl_name ;
        var log_tbl_seq_name;
		var statement;
		var result;
        var statement1;
        var result_set1;
        var records;
        var query_get_records;
        var get_rec_rs;
        var stmt;
        var rs;
        var del_query;
        var del_stmt;
        var del_rs;
        var ins_query;
        var ins_stmt;
        var ins_rs;
        module_load_schema = db_name + '.load_cda_' + MODULE;
        module_stage_schema = db_name + '.stage_' + MODULE;
		module_warehouse_schema = db_name + '.dw_' + MODULE;
		log_tbl_name = common_load_schema + '.cda_load_log';
        log_tbl_seq_name = common_load_schema + '.cda_load_log_seq';
		log_detail_tbl_name = common_load_schema + '.cda_load_log_detail';
		
		proc_start_ts = new Date().getTime();
		module_load_status = 'START';
        //run_id = 1;
        //task_name = 'Loading';
       
    
		/* *********************************************************************************
					 Begin processing
		************************************************************************************ */
        try {
      
        task_name = MODULE + '_' + TABLE_PREFIX;
        task_name = task_name.toUpperCase();
        query = 'select ' + log_tbl_seq_name + '.nextval;';
        statement = snowflake.createStatement( {sqlText: query} );  
        rs = statement.execute();
        
        while(rs.next()){
            run_id = rs.getColumnValue(1);
        }
        
        query = 'insert into identifier(:1) (run_id,task_name, load_ts, status, proc_start_ts) values(:2 , :3 , to_timestamp_ltz(\''+load_ts+'\') , :4 ,to_timestamp_ltz(\''+proc_start_ts+'\'))';
        statement = snowflake.createStatement( 
        {sqlText: query,
        binds: [log_tbl_name,run_id, task_name   , module_load_status ]
        } );  
        result = statement.execute();
                
        log(log_detail_tbl_name,run_id, task_name, module_load_status ,load_ts, null, 0);       
        
		/* *********************************************************************************
					 Get list of tables to process
		************************************************************************************ */
		
		//-- Get all the latest entries from the Manifest files that were not loaded by comparing with the last savepoint entry
		//query = 'select distinct m.tbl_name, split_part(m.data_file_path, \'/\', 4) as s3_key, m.last_successful_write_ts_ms as last_manifest_ts, nvl(s.last_successful_load_ts_ms,0) as last_savepoint_ts, nvl2(s.tbl_name, false, true) as is_new_table from ' + module_load_schema + '.manifest m left outer join ' + module_load_schema + '.savepoints s on s.tbl_name = m.tbl_name where  m.last_successful_write_ts_ms > nvl(s.last_successful_load_ts_ms, 0) and split_part(m.tbl_name, \'_\', 0) = \'' + TABLE_PREFIX + '\' limit '+ BATCH_SIZE + ';';
		
		query = 'select distinct m.tbl_name, split_part(m.data_file_path, \'/\', 4) as s3_key, m.last_successful_write_ts_ms as last_manifest_ts, nvl(s.last_successful_load_ts_ms,0) as last_savepoint_ts, nvl2(s.tbl_name, false, true) as is_new_table from ' + module_load_schema + '.manifest m left outer join ' + module_load_schema + '.savepoints s on s.tbl_name = m.tbl_name where m.tbl_name like \'%' + TABLE_PREFIX + '%\' limit '+ BATCH_SIZE + ';'; //Apisero testing comment.
		
		//query = 'select distinct m.tbl_name, split_part(m.data_file_path, \'/\', 4) as s3_key, m.last_successful_write_ts_ms as last_manifest_ts, nvl(s.last_successful_load_ts_ms,0) as last_savepoint_ts, nvl2(s.tbl_name, false, true) as is_new_table from ' + module_load_schema + '.manifest m left outer join ' + module_load_schema + '.savepoints s on s.tbl_name = m.tbl_name where m.last_successful_write_ts_ms > nvl(s.last_successful_load_ts_ms, 0) and m.tbl_name = \'' + TABLE_PREFIX + '\' limit '+ BATCH_SIZE + ';'; //-- Uncomment for Testing
        
		var statement = snowflake.createStatement( {sqlText: query} );  
        var proc_tbl = statement.execute();
        //return query;
		log(log_detail_tbl_name,run_id, task_name, 'Get new entries from Manifest file', load_ts, null,1);
        //return result;
		//-- For every table (a row) in the manifest file
		while(proc_tbl.next()){  
            
			proc_tbl.is_new_table = proc_tbl.getColumnValue("IS_NEW_TABLE");
            proc_tbl.tbl_name = proc_tbl.getColumnValue("TBL_NAME"); 
            proc_tbl.s3_key = proc_tbl.getColumnValue("S3_KEY");
            proc_tbl.last_successful_load_ts_ms = proc_tbl.getColumnValue("LAST_MANIFEST_TS");
            proc_tbl.last_manifest_ts = proc_tbl.getColumnValue("LAST_MANIFEST_TS");
            proc_tbl.last_savepoint_ts = proc_tbl.getColumnValue("LAST_SAVEPOINT_TS");
			tbl_name = proc_tbl.tbl_name;
			tbl_rows_parsed = 0;
			tbl_rows_loaded = 0;
			tbl_errors_seen = 0;
			tbl_proc_start_ts = new Date().getTime();
            
            log(log_detail_tbl_name,run_id, task_name, 'Table Load Start', load_ts, tbl_name, null);
            
			if (proc_tbl.is_new_table) { 
				//-- Create a new table to save the raw data as VARIANT
				query = 'create or replace table ' + module_load_schema + '.' + proc_tbl.tbl_name + ' (src_data variant, __src_file varchar, __load_ts timestamp_ntz, run_id int);';
				statement1 = snowflake.createStatement( {sqlText: query} );
                result_set1 = statement1.execute();
				log(log_detail_tbl_name,run_id, task_name, 'Create new Load Table',load_ts, tbl_name,1);
                
			}
          
			/* *********************************************************************************
				Copy the raw data from external storage to the table created from the above step
			************************************************************************************ */
			
			folder_list_query = 'list @' + module_load_schema + '.xstg_s3_germ_cda_' + MODULE + '/' + proc_tbl.s3_key + '/ pattern = \'.*\\.parquet\';';
			statement1 = snowflake.createStatement( {sqlText: folder_list_query} );  
            rs_folder_list = statement1.execute();
			//-- For each table get the all new timestamp folders that can be copied
			//-- folder timestamp > savepoint last loaded timestamp and <= manifest last successful write timestamp

			folder_list_query = 'select tbl_name, folder_ts from (select distinct split_part("name", \'/\', 4) tbl_name, split_part("name", \'/\', 6) folder_ts from table(result_scan(last_query_id())) where folder_ts > ' + proc_tbl.last_savepoint_ts + ' and folder_ts <= ' + proc_tbl.last_manifest_ts + ');';
			
			var statement1 = snowflake.createStatement( {sqlText: folder_list_query} );  
            rs_folder_list = statement1.execute();
 
			//--return table(rs_folder_list);
			log(log_detail_tbl_name,run_id, task_name, 'Get new timestamp folders', load_ts, tbl_name,1);
            
		    folder_list = '';
			 while (rs_folder_list.next()) {
             folder_ts = rs_folder_list.getColumnValue('FOLDER_TS');
				folder_list = folder_list + folder_ts + '|';
			}
			folder_list = folder_list.slice(0, -1); 

			query = 'copy into ' + module_load_schema + '.' + proc_tbl.tbl_name + ' from (select $1, metadata$filename, \'' + load_ts + '\','+ run_id +' from @' + module_load_schema + '.xstg_s3_germ_cda_' + MODULE + '/' + proc_tbl.s3_key + '/) pattern = \'.*(' + folder_list + ').*\\.parquet\' file_format = ' + common_load_schema + '.ff_germ_cda_parquet force = true;';
			var statement1 = snowflake.createStatement( {sqlText: query} );  
            var result = statement1.execute();

			/* *********************************************************************************
						Process the results of COPY statement for logging
			********************************************************************************* */

			query = 'select sum("rows_parsed") "tbl_rows_parsed", sum("rows_loaded") "tbl_rows_loaded", sum("errors_seen") "tbl_errors_seen", listagg(to_json(object_construct(*)), \',\') "tbl_copy_result_json" from table(result_scan(last_query_id()))';
            var statement1 = snowflake.createStatement( {sqlText: query} );  
            
            rs_copy_result = statement1.execute();
            
			while (rs_copy_result.next()){
				tbl_rows_parsed = rs_copy_result.getColumnValue("tbl_rows_parsed");
				tbl_rows_loaded = rs_copy_result.getColumnValue("tbl_rows_loaded");
				tbl_errors_seen = rs_copy_result.getColumnValue("tbl_errors_seen");
			}
			log(log_detail_tbl_name,run_id, task_name, 'COPY INTO', load_ts, tbl_name,1);            
           
			/* *********************************************************************************
				Create a view on top of the newly created load table to show the flattened data  
			********************************************************************************* */
			
			//-- Start bulding the view query
			create_view_query = 'create or replace view ' + module_stage_schema + '.' + proc_tbl.tbl_name + ' as select';

			//-- Get Column name and type from Parquet files using infer_schema
			query = 'select column_name, type from table(infer_schema(location=>\'@' + module_load_schema + '.xstg_s3_germ_cda_' + MODULE + '/' + proc_tbl.s3_key + '/\', file_format=>\'' + common_load_schema + '.ff_germ_cda_parquet\'));';
			var statement1 = snowflake.createStatement( {sqlText: query} );  
            rs_infer_schema = statement1.execute();
          
			log(log_detail_tbl_name,run_id, task_name,'Infer Table Schema',load_ts, tbl_name,1);
            
			  //Execution error in store procedure LOAD_CDA_JAVASCRIPT: Error parsing JSON: more than one document in the input At Statement.execute, line 216 position 31
            
			//-- To the view query, add a JSON column parser logic for every column in the table
			 while (rs_infer_schema.next()){
				rs_infer_schema.type = rs_infer_schema.getColumnValue("TYPE");
                rs_infer_schema.column_name = rs_infer_schema.getColumnValue("COLUMN_NAME");
                if(rs_infer_schema.type = 'BINARY'){
                    var parType = 'TEXT';
                }else{
                    var parType = rs_infer_schema.type;
                }
				create_view_query = create_view_query + ' t.src_data:' + rs_infer_schema.column_name + '::' + parType + ' as ' + rs_infer_schema.column_name + ',';
			}
            
			//-- Complete and execute the view query
			create_view_query = create_view_query + ' split_part(__src_file,\'/\',2) as __fingerprintfolder, split_part(__src_file,\'/\',3) as __timestampfolder, __load_ts , run_id';
			create_view_query = create_view_query + ' from ' + module_load_schema + '.' + proc_tbl.tbl_name + ' t;';
			var statement1 = snowflake.createStatement( {sqlText: create_view_query} );
            var result_set1 = statement1.execute();
			log(log_detail_tbl_name,run_id, task_name,'Create Load View', load_ts, tbl_name,1);
            
			
			/* *********************************************************************************
						Load the data load schema to warehouse schema
			********************************************************************************* */
			//-- If historical load or a new file found during incremental, directly create the DW table
			//-- Otherwise, for incremental, create a swap table with new data, and swap original DW tabl;e with it
          
			if (proc_tbl.is_new_table) { 
			
                dw_merge_query = '(select * from (select *, row_number() over (partition by id order by __LOAD_TS desc) as __row_num from ' 
                + module_stage_schema + '.' + proc_tbl.tbl_name + ') where __row_num = 1 and gwcbi___operation != 1)';

				//-- Create the table in warehouse schema and populate with the merged data from the Load Schema rawdata (via View created)
				query = 'create or replace table ' + module_warehouse_schema + '.' + proc_tbl.tbl_name + ' as ' + dw_merge_query + ';';
				statement1 = snowflake.createStatement( {sqlText: query} );
                result_set1 = statement1.execute();
				
                log(log_detail_tbl_name,run_id, task_name,'Create new DW Table', load_ts, tbl_name,1);                
				 
				//-- Create a new Savepoint entry for the new table
				query = 'insert into ' + module_load_schema + '.savepoints values ' + '(\'' + proc_tbl.tbl_name + '\', ' + proc_tbl.last_manifest_ts + ', \'' + proc_start_ts + '\')';
				statement1 = snowflake.createStatement( {sqlText: query} );
                result_set1 = statement1.execute();      
				log(log_detail_tbl_name,run_id, task_name,'Make new Savepoint entry',load_ts, tbl_name,1);
                
			}    
			else{
				
                query_get_records = 'select * from (select *, row_number() over (partition by id order by __LOAD_TS desc) as __row_num from ' 
                + module_stage_schema + '.' + proc_tbl.tbl_name + ') where __row_num = 1 and run_id = ' + run_id;
                statement = snowflake.createStatement( {sqlText: query_get_records} );  
                get_rec_rs = statement.execute();
                //return query_get_records;
                //log(log_detail_tbl_name,run_id, task_name, 'Get records IDs to load', load_ts, tbl_name, 1);
                
                
                while(get_rec_rs.next()) {
                    records = get_rec_rs.getColumnValue("GWCBI___OPERATION");
                    var id_value = get_rec_rs.getColumnValue("ID");
                    if(records == 1){
                    
                        query = 'delete from  ' + module_warehouse_schema + '.' + proc_tbl.tbl_name + ' where id = ' + id_value + ';';        
                        stmt = snowflake.createStatement( {sqlText: query} );  
                        rs = stmt.execute();
                        
                        //rows_parsed = rs.getColumnValue(1);
                        //log(log_detail_tbl_name,run_id, task_name,'Record deleted', load_ts, tbl_name, 1);                                             
                    }
                    else if(records == 2){
                        
                        query = 'insert into ' + module_warehouse_schema + '.' + proc_tbl.tbl_name + ' ' + query_get_records +' and gwcbi___operation != 1 and GWCBI___OPERATION = 2 and id = ' + id_value + ';';        
                        stmt = snowflake.createStatement( {sqlText: query} );  
                        rs = stmt.execute();
                        
                        //log(log_detail_tbl_name,run_id, task_name,'Record inserted',  load_ts, tbl_name,1);                      
                    }
                    else if(records == 4){
                    
                        del_query = 'delete from  ' + module_warehouse_schema + '.' + proc_tbl.tbl_name + ' where id = ' + id_value + ';';        
                        del_stmt = snowflake.createStatement( {sqlText: del_query} );  
                        del_rs = del_stmt.execute();
                        
                        ins_query = 'insert into ' + module_warehouse_schema + '.' + proc_tbl.tbl_name + ' ' + query_get_records +' and gwcbi___operation != 1 and GWCBI___OPERATION = 4 and id = ' + id_value + ';';        
                        ins_stmt = snowflake.createStatement( {sqlText: ins_query} );  
                        ins_rs = ins_stmt.execute();  
                        
                        //log(log_detail_tbl_name,run_id, task_name,'Record updated',load_ts, tbl_name,1);                       
                    } 
                }

				//-- Update savepoints table for the changed table
				query = 'update ' + module_load_schema + '.savepoints set last_successful_load_ts_ms = \'' + proc_tbl.last_manifest_ts + '\', __update_ts = \'' + proc_start_ts + '\' where tbl_name = \'' + proc_tbl.tbl_name + '\';';
				var statement1 = snowflake.createStatement( {sqlText: query} );
                var result_set1 = statement1.execute();     
				log(log_detail_tbl_name,run_id, task_name,'Update Savepoint entry', load_ts, tbl_name,1);
                
			}
            
			/* *********************************************************************************
					Continue to prepare the data for logging
			********************************************************************************* */
			rows_parsed = rows_parsed + tbl_rows_parsed;
			rows_loaded = rows_loaded + tbl_rows_loaded;
			errors_seen = errors_seen + tbl_errors_seen;    
			module_tbl_count = module_tbl_count + 1;
			tbl_proc_end_ts = new Date().getTime();
			
            query = 'update identifier(:1) set  rows_parsed = :2, rows_loaded = :3, errors_seen = :4, table_count = :5 where run_id = :6';
		    statement = snowflake.createStatement( 
            {sqlText: query,
            binds: [log_tbl_name , rows_parsed , rows_loaded, errors_seen,module_tbl_count,run_id ] 
            } );  
            result = statement.execute();
            
            log(log_detail_tbl_name,run_id, task_name, 'Table Load Finish', load_ts, tbl_name, null);          
		}
		
		/* *********************************************************************************
				   Complete processing
		********************************************************************************* */
		module_load_status = 'SUCCEEDED';
		proc_end_ts = new Date().getTime();

		//-- Update the row for module load status in load log table
		query = 'update identifier(:1) set status = :2, rows_parsed = :3, rows_loaded = :4, errors_seen = :5, table_count = :6, proc_end_ts = to_timestamp_ltz(\''+proc_end_ts+'\') where run_id = :7';
		statement = snowflake.createStatement( {sqlText: query,
        binds: [log_tbl_name,module_load_status , rows_parsed , rows_loaded, errors_seen,module_tbl_count,run_id ] 
        } );  
        result = statement.execute();
        
        log(log_detail_tbl_name,run_id, task_name, module_load_status ,load_ts, null, 0);
        
        return module_load_status;
        }
        catch(err)
		{                
	        try
			{
                
            result =  '{ \'Failed: Code\': \'' + err.code + '\',  \'State\': \'' + err.state;
			result += '\',  \'Message\': \'' + err.message.replace(/['"()]+/g,'');
			result += '\', \'Stack Trace\':\'' + err.stackTraceTxt.replace(/['"()]+/g,'') + '\'}'; 
           
                module_load_status = 'Failed';
                proc_end_ts = new Date().getTime();
				  query = 'insert into identifier(:1) select :2, :3, to_timestamp_ntz(\''+load_ts+'\'), current_timestamp(), \'INFO\', null, :4, :5, parse_json(concat(\'[\',listagg(to_json('+ result +'), \',\'),\']\'));';
                 statement = snowflake.createStatement( 
                 {sqlText: query,
                    binds: [log_detail_tbl_name,run_id, task_name  ,module_load_status,result] 
                     } );  
                result = statement.execute();
                //return query;
                query = 'update identifier(:1) set status = :2, rows_parsed = :3, rows_loaded = :4, errors_seen = :5, table_count = :6, proc_end_ts = to_timestamp_ltz(\''+proc_end_ts+'\') where run_id = :7';
		        statement = snowflake.createStatement( 
                {sqlText: query,
                binds: [log_tbl_name,module_load_status , rows_parsed , rows_loaded, errors_seen,module_tbl_count,run_id ] 
                } );  
                result = statement.execute();
		    }
			catch(e)
			{
                result +=  "\n "+"Failed: Code:  ERROR while inserting data to LOG table  " + e.code + "\n  State: " + e.state;
				result += "\n  Message: " + e.message;
				result += "\nStack Trace:\n" + e.stackTraceTxt;
			}
			return module_load_status + " : " + err.message;
           
		}
  
	$$
;
