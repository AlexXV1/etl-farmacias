# Demostración de falla controlada (exigida por la pauta). PowerShell.
#
#   .\scripts\demo_falla.ps1 -Paso provocar    # URL incorrecta en la Variable + dispara AMBOS DAGs de fuente
#   .\scripts\demo_falla.ps1 -Paso estado      # estado de las tareas en Airflow + auditoría de la demo
#   .\scripts\demo_falla.ps1 -Paso recuperar   # restaura la URL y vuelve a ejecutar etl_minsal
#
# Qué se debe ver:
#   - etl_minsal.validar_api: HTTP 404 -> up_for_retry x3 (esperas de 20 s, 40 s, 80 s) -> failed;
#     las tareas siguientes quedan upstream_failed: no se carga nada inválido.
#   - etl_comunas, disparado al mismo tiempo, termina en success: los DAGs son independientes.
#   - Cada intento queda como una fila 'fallido' en audit.etl_audit (on_failure/on_retry_callback).
#   - Al recuperar: etl_minsal en success y calcular_indicadores se dispara solo (asset_triggered).
param(
    [Parameter(Mandatory)] [ValidateSet("provocar", "estado", "recuperar")] [string] $Paso
)

$Raiz = Split-Path -Parent $PSScriptRoot
Set-Location $Raiz

# La URL correcta se lee de config/variables.json (misma fuente que usa airflow-init): no se duplica aquí.
$UrlCorrecta = (Get-Content (Join-Path $Raiz "config\variables.json") -Raw | ConvertFrom-Json).minsal_locales_url
$UrlIncorrecta = $UrlCorrecta -replace "getLocales\.php$", "getLocales_URL_INCORRECTA.php"
$Sufijo = Get-Date -Format "HHmmss"

function Airflow { docker compose exec -T airflow-scheduler airflow @args 2>$null }

switch ($Paso) {
    "provocar" {
        Airflow variables set minsal_locales_url $UrlIncorrecta | Out-Null
        Write-Host "Variable minsal_locales_url -> $UrlIncorrecta"
        Airflow dags trigger etl_minsal --run-id "demo_falla_minsal_$Sufijo" | Out-Null
        Airflow dags trigger etl_comunas --run-id "demo_comunas_$Sufijo" | Out-Null
        Write-Host "Disparados: demo_falla_minsal_$Sufijo y demo_comunas_$Sufijo  ($(Get-Date -Format HH:mm:ss))"
        Write-Host "`nObservar en http://127.0.0.1:8080:"
        Write-Host "  etl_minsal  -> validar_api en up_for_retry (reintentos a los 20 s, 40 s y 80 s) y luego failed"
        Write-Host "  etl_comunas -> success (no le afecta la falla de MINSAL)"
        Write-Host "Luego: .\scripts\demo_falla.ps1 -Paso estado"
    }
    "estado" {
        $actual = (Airflow variables get minsal_locales_url | Select-Object -Last 1).Trim()
        if ($actual -ne $UrlCorrecta) {
            Write-Host "ATENCIÓN: la Variable sigue con la URL incorrecta. Al terminar: -Paso recuperar" -ForegroundColor Yellow
        }
        foreach ($dag in "etl_minsal", "etl_comunas") {
            $run = (Airflow dags list-runs $dag -o json | Out-String | ConvertFrom-Json |
                    Where-Object { $_.run_id -like "demo*" -or $_.run_id -like "recuperacion*" } |
                    Select-Object -First 1)
            if ($run) {
                Write-Host "`n== $dag  $($run.run_id)  -> $($run.state)"
                Airflow tasks states-for-dag-run $dag $run.run_id -o json | Out-String | ConvertFrom-Json |
                    Select-Object task_id, state | Format-Table -AutoSize | Out-String | Write-Host
            }
        }
        Write-Host "== Auditoría (audit.etl_audit)"
        docker compose exec -T postgres psql -U etl_farmacias -d farmacias -c @"
SELECT dag_id, task_id, intento, estado, to_char(inicio AT TIME ZONE 'America/Santiago','HH24:MI:SS') AS inicio,
       left(mensaje, 80) AS mensaje
FROM audit.etl_audit
WHERE run_id LIKE 'demo%' OR run_id LIKE 'recuperacion%'
ORDER BY inicio DESC LIMIT 25;
"@
    }
    "recuperar" {
        Airflow variables set minsal_locales_url $UrlCorrecta | Out-Null
        Write-Host "Variable minsal_locales_url -> $UrlCorrecta"
        Airflow dags trigger etl_minsal --run-id "recuperacion_minsal_$Sufijo" | Out-Null
        Write-Host "Disparado: recuperacion_minsal_$Sufijo  ($(Get-Date -Format HH:mm:ss))"
        Write-Host "Al terminar, etl_minsal dispara calcular_indicadores automáticamente (asset_triggered__...)."
    }
}
