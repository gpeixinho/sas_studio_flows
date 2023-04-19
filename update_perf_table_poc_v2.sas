proc sql;
	select ldttm format=BEST. into :ldttm 
	from data.cas_enrichment_perf_ldttm;
quit;

%put &ldttm;

data tb_enrichment_perf;
	set pgdb.tb_enrichment_perf;
	where dttm_request>&ldttm;
run;

proc ds2;
	data work.perf_dttm(overwrite=yes);
		/* data grid object */

		dcl package datagrid input_grid();
		dcl integer rowNumber;
		dcl integer columnCount;
		dcl integer i;
		dcl integer rc;
		dcl integer dg_size;
                dcl varchar(100) FONTE;
                dcl double TEMPO;
				dcl varchar(100) STATUS;
				dcl varchar(100) ORIGEM;
				dcl varchar(100) REQUEST_ID;

		%dcm_datagrid_interface() ;

		method run();
			set work.tb_enrichment_perf;
                        datagrid_create(input_grid, perf_data);
                        dg_size = datagrid_count(input_grid);

			do i=1 to dg_size;
                		FONTE=DataGrid_get(input_grid, 'FONTE', i);
						TEMPO=DataGrid_get(input_grid, 'TEMPO', i);
						STATUS=DataGrid_get(input_grid, 'STATUS', i);
						ORIGEM=DataGrid_get(input_grid, 'ORIGEM', i);
						REQUEST_ID=DataGrid_get(input_grid, 'REQUEST_ID', i);
						output work.perf_dttm;
			end;
			/*Reset for next row.*/
			rc = DataGrid_clearData(input_grid);
		end;
	enddata;
	run;
quit;

data work.perf_dttm;
	set work.perf_dttm(drop=i rc dg_size perf_data  rownumber columncount);
run;

cas mysess;
caslib _all_ assign;

data siddata.tb_enrichment_perf_cas(append=force);
	set perf_dttm;
run;

proc sql outobs=1 noprint;
	SELECT dttm_request+1 format=BEST. INTO :ldttm
	FROM perf_dttm
	ORDER BY dttm_request DESC;
quit;

proc sql;
	delete from data.cas_enrichment_perf_ldttm;
	insert into data.cas_enrichment_perf_ldttm (ldttm)
	values (&ldttm);
quit; 

cas mysess terminate;
