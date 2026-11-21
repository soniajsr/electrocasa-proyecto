# ElectroCasa Lakehouse - Plataforma de Datos en Azure Databricks

## 1. Contexto y Arquitectura
Plataforma analítica basada en arquitectura Medallion sobre Azure Databricks utilizando Databricks Asset Bundles (DAB), Lakeflow Spark Declarative Pipelines (Auto Loader para archivos CSV/JSON) y Lakeflow Connect (conector query-based incremental desde Azure SQL `dbo.TrackingEnvios`).

## 2. Pasos de Despliegue
1. Crear los grupos en **Settings > Identity and access > Groups**: `electrocasa_ingenieria`, `electrocasa_analistas`, `electrocasa_auditoria` y añadir al usuario propietario a `electrocasa_ingenieria`.
2. Configurar la conexión a Azure SQL en **Catalog > External Data > Connections** y crear el conector query-based de Lakeflow Connect apuntando a `<catalogo>.bronze.tracking_envios`.
3. Ejecutar `notebooks/00_setup.ipynb` parametrizado con el catálogo objetivo (`electrocasa_dev` o `electrocasa_prod`).
4. Validar y desplegar el bundle:
   ```bash
   databricks bundle validate -t dev
   databricks bundle deploy -t dev
   databricks bundle run electrocasa_job -t dev
   ```

## 3. Evidencias de Ejecución y Gobierno
- Conteos por lote (Lote 1, Lote 2, Lote 3) en Bronze, Silver y Cuarentena.
- Evidencia de las dos corridas incrementales del conector de Tracking.
- Evidencia de `GRANT`, `REVOKE` y `SHOW GRANTS`, además de enmascaramiento dinámico (`MASK`) sobre `dni` y `salario`.
