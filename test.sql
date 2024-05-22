CREATE OR REPLACE PROCEDURE kwdm.execute_procedure(statemet text)
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $procedure$
declare
begin
	execute (statemet);
	RAISE NOTICE 'statemet: %', statemet;
end
$procedure$
;
