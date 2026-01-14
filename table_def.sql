SELECT
  'pg_dump --schema-only -t '
  || quote_ident(n.nspname) || '.'
  || quote_ident(c.relname)
  || ' dbname >> tables_ddl.sql'
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'schema1'
  AND c.relname IN (
      'table1','table2','table3','table4','table5',
      'table6','table7','table8','table9','table10'
  );