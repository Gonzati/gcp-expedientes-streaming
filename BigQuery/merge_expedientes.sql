MERGE `Proyecto.Expedientes_OLAP.Expedientes` AS T
USING (
  SELECT
    T.Ref,

    -- Cuantía combinada: T + Staging (agrupado), sin duplicados y ordenada
    ARRAY(
      SELECT AS STRUCT importe, timestamp
      FROM (
        SELECT c.importe, c.timestamp
        FROM UNNEST(T.Cuantia) AS c

        UNION DISTINCT

        SELECT c2.importe, c2.timestamp
        FROM UNNEST(S_cuantia) AS c2
      )
      ORDER BY timestamp
    ) AS merged_cuantia,

    -- Estado combinado: T + Staging (agrupado), sin duplicados y ordenado
    ARRAY(
      SELECT AS STRUCT estado, timestamp
      FROM (
        SELECT e.estado, e.timestamp
        FROM UNNEST(T.Estado) AS e

        UNION DISTINCT

        SELECT e2.estado, e2.timestamp
        FROM UNNEST(S_estado) AS e2
      )
      ORDER BY timestamp
    ) AS merged_estado

  FROM `Proyecto.Expedientes_OLAP.Expedientes` AS T
  JOIN (
    -- Agrupamos staging para tener UNA sola fila por Ref
    SELECT
      Ref,
      ARRAY_CONCAT_AGG(Cuantia) AS S_cuantia,
      ARRAY_CONCAT_AGG(Estado)  AS S_estado
    FROM `Proyecto.Expedientes_OLAP.Expedientes_Staging`
    GROUP BY Ref
  ) AS S
  USING (Ref)
) AS M
ON T.Ref = M.Ref

WHEN MATCHED THEN
  UPDATE SET
    Cuantia = M.merged_cuantia,
    Estado  = M.merged_estado;
