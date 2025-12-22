#!/usr/bin/env python3
"""
Script para extraer datos del Core Update de diciembre 2025
Compara impresiones de Maps y Search ANTES vs DESPUÉS del 11 de diciembre

Períodos de comparación:
- ANTES: Dec 4-10, 2025 (7 días)
- DESPUÉS: Dec 11-17, 2025 (7 días)
"""

import singlestoredb as s2
import json
from datetime import datetime

# Credenciales
USER = 'searchmas'
PASSWORD = 'gT#8V!mPzR2x@WdL'
DATABASE = 'tenants'
PORT = 3306

SERVERS = [
    ('prd-1', 'client-reporting-prd-1.meetsoci.com'),
    ('prd-2', 'client-reporting-prd-2.meetsoci.com'),
    ('prd-3', 'client-reporting-prd-3.meetsoci.com')
]

# Períodos de comparación
BEFORE_START = '2025-12-04'
BEFORE_END = '2025-12-10'
AFTER_START = '2025-12-11'
AFTER_END = '2025-12-17'

def get_tenant_data(cursor, tenant_name):
    """Extrae datos de impresiones para un tenant específico"""
    table = f"{tenant_name}_gmb_location_metrics"

    query = f"""
    SELECT
        '{tenant_name}' as client,
        -- ANTES del Core Update (Dec 4-10)
        SUM(CASE WHEN date BETWEEN '{BEFORE_START}' AND '{BEFORE_END}'
            THEN COALESCE(total_business_impressions_maps, 0) ELSE 0 END) as maps_before,
        SUM(CASE WHEN date BETWEEN '{BEFORE_START}' AND '{BEFORE_END}'
            THEN COALESCE(total_business_impressions_search, 0) ELSE 0 END) as search_before,
        -- DESPUÉS del Core Update (Dec 11-17)
        SUM(CASE WHEN date BETWEEN '{AFTER_START}' AND '{AFTER_END}'
            THEN COALESCE(total_business_impressions_maps, 0) ELSE 0 END) as maps_after,
        SUM(CASE WHEN date BETWEEN '{AFTER_START}' AND '{AFTER_END}'
            THEN COALESCE(total_business_impressions_search, 0) ELSE 0 END) as search_after,
        -- Número de ubicaciones únicas
        COUNT(DISTINCT remote_id) as locations
    FROM {table}
    WHERE date BETWEEN '{BEFORE_START}' AND '{AFTER_END}'
    """

    cursor.execute(query)
    return cursor.fetchone()

def calculate_variation(before, after):
    """Calcula la variación porcentual"""
    if before is None or after is None:
        return None
    if before == 0:
        if after == 0:
            return 0.0
        return None  # No se puede calcular
    return round(((after - before) / before) * 100, 2)

def get_verdict(variation, threshold=15):
    """Determina el veredicto basado en la variación"""
    if variation is None:
        return 'SIN DATOS'
    if variation <= -threshold:
        return 'AFECTADO NEG'
    elif variation >= threshold:
        return 'AFECTADO POS'
    else:
        return 'NO AFECTADO'

def main():
    all_clients = []

    print("🚀 EXTRACCIÓN DE DATOS - CORE UPDATE DICIEMBRE 2025")
    print("=" * 60)
    print(f"📅 Período ANTES: {BEFORE_START} a {BEFORE_END} (7 días)")
    print(f"📅 Período DESPUÉS: {AFTER_START} a {AFTER_END} (7 días)")
    print("=" * 60)

    for server_name, server_host in SERVERS:
        print(f"\n🔌 Conectando a {server_name}: {server_host}")

        try:
            conn = s2.connect(
                host=server_host,
                port=PORT,
                user=USER,
                password=PASSWORD,
                database=DATABASE,
                connect_timeout=30
            )

            with conn.cursor() as cursor:
                # Obtener lista de tenants con gmb_location_metrics
                cursor.execute("SHOW TABLES LIKE '%gmb_location_metrics'")
                tables = cursor.fetchall()

                tenants = [t[0].replace('_gmb_location_metrics', '') for t in tables]
                print(f"   📊 Encontrados {len(tenants)} tenants")

                processed = 0
                errors = 0

                for tenant in tenants:
                    try:
                        result = get_tenant_data(cursor, tenant)

                        if result:
                            client, maps_before, search_before, maps_after, search_after, locations = result

                            # Solo incluir si tiene datos
                            if (maps_before or maps_after or search_before or search_after):
                                maps_var = calculate_variation(maps_before, maps_after)
                                search_var = calculate_variation(search_before, search_after)

                                client_data = {
                                    'client': client,
                                    'maps_before': int(maps_before or 0),
                                    'search_before': int(search_before or 0),
                                    'maps_after': int(maps_after or 0),
                                    'search_after': int(search_after or 0),
                                    'maps_var': maps_var,
                                    'search_var': search_var,
                                    'locations': int(locations or 0),
                                    'maps_verdict': get_verdict(maps_var),
                                    'search_verdict': get_verdict(search_var),
                                    'server': server_name
                                }
                                all_clients.append(client_data)
                                processed += 1
                    except Exception as e:
                        errors += 1
                        if errors <= 3:
                            print(f"   ⚠️ Error en {tenant}: {str(e)[:50]}")

                print(f"   ✅ Procesados: {processed} tenants (errores: {errors})")

            conn.close()

        except Exception as e:
            print(f"   ❌ Error de conexión: {str(e)[:80]}")

    # Resumen
    print("\n" + "=" * 60)
    print("📊 RESUMEN FINAL")
    print("=" * 60)
    print(f"Total clientes procesados: {len(all_clients)}")

    # Contar veredictos
    maps_neg = sum(1 for c in all_clients if c['maps_verdict'] == 'AFECTADO NEG')
    maps_pos = sum(1 for c in all_clients if c['maps_verdict'] == 'AFECTADO POS')
    maps_no = sum(1 for c in all_clients if c['maps_verdict'] == 'NO AFECTADO')

    search_neg = sum(1 for c in all_clients if c['search_verdict'] == 'AFECTADO NEG')
    search_pos = sum(1 for c in all_clients if c['search_verdict'] == 'AFECTADO POS')
    search_no = sum(1 for c in all_clients if c['search_verdict'] == 'NO AFECTADO')

    print(f"\n📍 MAPS:")
    print(f"   🔴 Afectados negativamente: {maps_neg}")
    print(f"   🟢 Afectados positivamente: {maps_pos}")
    print(f"   ⚪ No afectados: {maps_no}")

    print(f"\n🔍 SEARCH:")
    print(f"   🔴 Afectados negativamente: {search_neg}")
    print(f"   🟢 Afectados positivamente: {search_pos}")
    print(f"   ⚪ No afectados: {search_no}")

    # Guardar datos
    output_file = '/Users/mariova/Documents/searchmas/core-update-report/client_data_updated.json'
    with open(output_file, 'w') as f:
        json.dump(all_clients, f, indent=2)
    print(f"\n💾 Datos guardados en: {output_file}")

    # Generar código JavaScript para el dashboard
    js_output = '/Users/mariova/Documents/searchmas/core-update-report/clientData_updated.js'
    with open(js_output, 'w') as f:
        f.write("// Datos actualizados del Core Update - Generado: " + datetime.now().strftime("%Y-%m-%d %H:%M") + "\n")
        f.write(f"// Período ANTES: {BEFORE_START} a {BEFORE_END}\n")
        f.write(f"// Período DESPUÉS: {AFTER_START} a {AFTER_END}\n\n")
        f.write("const clientData = [\n")

        for c in all_clients:
            f.write(f"{{client:'{c['client']}',")
            f.write(f"maps_before:{c['maps_before']},search_before:{c['search_before']},")
            f.write(f"maps_after:{c['maps_after']},search_after:{c['search_after']},")
            f.write(f"maps_var:{c['maps_var'] if c['maps_var'] is not None else 'null'},")
            f.write(f"search_var:{c['search_var'] if c['search_var'] is not None else 'null'},")
            f.write(f"locations:{c['locations']},")
            f.write(f"maps_verdict:'{c['maps_verdict']}',search_verdict:'{c['search_verdict']}'}},\n")

        f.write("];\n")

    print(f"💾 JavaScript guardado en: {js_output}")

    return all_clients

if __name__ == "__main__":
    main()
