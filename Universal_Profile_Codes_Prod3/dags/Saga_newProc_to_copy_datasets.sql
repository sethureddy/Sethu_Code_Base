CREATE PROCEDURE `Saga_new.copy_tables`(source_dataset_name STRING, destination_dataset_name STRING)
BEGIN
  DECLARE table_list ARRAY<STRING>;
  DECLARE i INT64 DEFAULT 0;
  DECLARE table_name STRING;
  DECLARE drop_sql STRING;
  DECLARE copy_sql STRING;
  DECLARE partition_cols STRING;
  DECLARE partition_clause STRING;
  DECLARE partitioning_scheme STRING;
  DECLARE clustering_cols STRING;
  DECLARE clustering_clause STRING;
  DECLARE clustering_order STRING;
  
  SET table_list = (
      SELECT ARRAY_AGG(table_name)
      FROM `Saga_new`.`INFORMATION_SCHEMA.TABLES`
      WHERE  table_schema = source_dataset_name
        AND table_type = 'BASE TABLE'
  );

  WHILE i < ARRAY_LENGTH(table_list) DO
    SET table_name = table_list[OFFSET(i)];
    
    -- Get partitioning scheme of source table
    SET partitioning_scheme = (
      SELECT partitioning_type
      FROM `Saga_new`.`INFORMATION_SCHEMA`.`TABLES`
      WHERE table_name = table_name
        AND table_schema = source_dataset_name
    );
    
    -- Build partitioning clause for CREATE TABLE statement
    IF partitioning_scheme IS NOT NULL THEN
      SET partition_cols = (
        SELECT STRING_AGG(DISTINCT column_name, ', ')
        FROM `Saga_new`.`INFORMATION_SCHEMA`.`COLUMNS`
        WHERE table_name = table_name
          AND table_schema = source_dataset_name
          AND is_partitioning_column = 'YES'
      );
      SET partition_clause = CONCAT('PARTITION BY ', partitioning_scheme, '(', partition_cols, ')');
    ELSE
      SET partition_clause = '';
    END IF;
    
    -- Get clustering columns of source table
    SET clustering_cols = (
      SELECT STRING_AGG(DISTINCT column_name, ', ')
FROM (
  SELECT column_name
  FROM `Saga_new.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = 'Tests'
    AND table_schema = 'Saga_new'
    AND clustering_ordinal_position IS NOT NULL
  ORDER BY clustering_ordinal_position ASC
)
    );
    
    -- Build clustering clause for CREATE TABLE statement
    IF clustering_cols IS NOT NULL THEN
      SET clustering_order = (
        SELECT STRING_AGG(DISTINCT CONCAT(column_name, ' ASC'), ', ')
        FROM `Saga_new`.`INFORMATION_SCHEMA`.`COLUMNS`
        WHERE table_name = table_name
          AND table_schema = source_dataset_name
          AND clustering_ordinal_position IS NOT NULL
        ORDER BY clustering_ordinal_position ASC
      );
      SET clustering_clause = CONCAT('CLUSTER BY ', clustering_cols, ' ORDER BY ', clustering_order);
    ELSE
      SET clustering_clause = '';
    END IF;
    
    SET drop_sql=CONCAT(
        'DROP TABLE IF EXISTS `', destination_dataset_name, '.', table_name, '` ');
    EXECUTE IMMEDIATE drop_sql;  
    
    SET copy_sql = CONCAT(
        'CREATE TABLE `', destination_dataset_name, '.', table_name, '` ',
        partition_clause, ' ',
        clustering_clause, ' ',
        ' AS SELECT * FROM `', source_dataset_name, '.', table_name, '`'
    );
    EXECUTE IMMEDIATE copy_sql;
    
    SET i = i + 1;
  END WHILE;
END;
