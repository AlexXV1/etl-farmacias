# Genera el .zip de entrega a partir del ÚLTIMO COMMIT (git archive) y verifica que no contenga secretos.
# Uso (desde cualquier carpeta):  .\scripts\empaquetar.ps1
#
# Por qué git archive: el zip contiene exactamente lo versionado (lo mismo que hay en GitHub).
# .env, .venv, logs y cachés están en .gitignore, así que no pueden entrar.
$ErrorActionPreference = "Stop"
$raiz = Split-Path -Parent $PSScriptRoot
Set-Location $raiz
$nombre = "etl-farmacias"
$zip = Join-Path $raiz "$nombre.zip"     # *.zip está en .gitignore: el zip no se versiona

# 1. Aviso si hay cambios sin commitear: NO entrarían al zip.
$pendientes = git status --porcelain
if ($pendientes) {
    Write-Host "ATENCIÓN: hay cambios sin commitear que NO quedarán en el zip:" -ForegroundColor Yellow
    $pendientes | ForEach-Object { Write-Host "  $_" -ForegroundColor Yellow }
}

# 2. .env nunca debe estar versionado.
if (git ls-files .env) { throw ".env está versionado en Git: quitarlo (git rm --cached .env) antes de entregar" }

# 3. Generar el zip desde HEAD.
if (Test-Path $zip) { Remove-Item $zip -Confirm:$false }
git archive --format=zip --prefix="$nombre/" -o $zip HEAD
if ($LASTEXITCODE -ne 0) { throw "git archive falló" }

# 4. Descomprimir en una carpeta temporal y buscar cada secreto de .env dentro del contenido.
$tmp = Join-Path ([IO.Path]::GetTempPath()) "$nombre-verificacion-$(Get-Date -Format yyyyMMddHHmmss)"
Expand-Archive $zip -DestinationPath $tmp
try {
    $archivos = Get-ChildItem $tmp -Recurse -File
    if ($archivos | Where-Object Name -eq ".env") { throw "El zip contiene un archivo .env" }
    if (Test-Path .env) {
        # Valores de claves con PASSWORD, SECRET o KEY (8+ caracteres). La URI de la Connection
        # contiene la contraseña ETL, así que también queda cubierta por FARMACIAS_DB_PASSWORD.
        $secretos = Get-Content .env | Where-Object { $_ -match '^\w*(PASSWORD|SECRET|KEY)\w*=(.{8,})$' } |
            ForEach-Object { ($_ -split '=', 2)[1].Trim() }
        foreach ($s in $secretos) {
            $hits = $archivos | Where-Object { Select-String -Path $_.FullName -SimpleMatch $s -Quiet }
            if ($hits) { throw "Un secreto de .env aparece en: $($hits.Name -join ', ')" }
        }
        Write-Host "OK: ninguno de los $($secretos.Count) secretos de .env aparece en el zip." -ForegroundColor Green
    } else {
        Write-Host "No hay .env local: se omitió la búsqueda de secretos." -ForegroundColor Yellow
    }
    $kb = [math]::Round((Get-Item $zip).Length / 1KB)
    Write-Host "Zip generado: $zip  ($($archivos.Count) archivos, $kb KB, commit $(git rev-parse --short HEAD))"
} finally {
    Remove-Item $tmp -Recurse -Force -Confirm:$false
}
