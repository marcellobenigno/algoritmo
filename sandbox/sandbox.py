import os
import time

from osgeo import ogr

from classes.area_caixa import AreaCaixa
from classes.arruamento import Arruamento
from classes.demanda import Demanda
from classes.testada import Testada

ogr.UseExceptions()

start_time = time.time()

area = 'area_1'

input_dir = f'data/input/{area}/'
output_dir = f'data/output/{area}/'

postes = f'{input_dir}layer_postes.geojson'
demandas = f'{input_dir}layer_demandas.geojson'
arruamento = f'{input_dir}layer_arruamento.geojson'
alinhamento_predial = f'{input_dir}layer_alinhamento_predial.geojson'
lote = f'{input_dir}layer_lote.geojson'

drv_gjs = ogr.GetDriverByName('GeoJSON')
ds_postes = drv_gjs.Open(postes)
ds_arruamento = drv_gjs.Open(arruamento)
ds_alinhamento_predial = drv_gjs.Open(alinhamento_predial)
ds_demandas = drv_gjs.Open(demandas)
ds_lote = drv_gjs.Open(lote)

# criando um banco na memória para manipular as camadas:
driver_associado = ogr.GetDriverByName('Memory')
ds_associado = driver_associado.CreateDataSource('ds_associado')

# obtendo as camadas de cada DataSource
layer_postes = ds_postes.GetLayer()
layer_arruamento = ds_arruamento.GetLayer()
layer_alinhamento_predial = ds_alinhamento_predial.GetLayer()
layer_demandas = ds_demandas.GetLayer()
layer_lotes = ds_lote.GetLayer()

# ds_associado.CopyLayer(layer_postes, 'layer_postes')
ds_associado.CopyLayer(layer_arruamento, 'layer_arruamento')
ds_associado.CopyLayer(layer_alinhamento_predial, 'layer_alinhamento_predial')
ds_associado.CopyLayer(layer_demandas, 'layer_demandas')

# OBS: se quiser gerar a camada da testada (centróides), é só mudar o nome, ex.: layer_lotesssss
ds_associado.CopyLayer(layer_lotes, 'layer_lotes')


def print_layer_head(layer):
    print("\nExecutando print_layer_head")
    layerDefinition = layer.GetLayerDefn()

    # Obtenha o primeiro Feature do Layer
    feature = layer.GetFeature(0)

    print("| {:<14} | {:<14} | {:<14}".format(
        "Nome", "Tipo", "Primeiro valor"))
    print("| {:<14} | {:<14} | {:<14}".format(
        "-" * 5, "-" * 5, "-" * 5, ))

    for i in range(layerDefinition.GetFieldCount()):
        fieldName = str(layerDefinition.GetFieldDefn(i).GetName())[:14]
        fieldTypeCode = layerDefinition.GetFieldDefn(i).GetType()
        fieldType = str(layerDefinition.GetFieldDefn(
            i).GetFieldTypeName(fieldTypeCode))

        # Obtenha o valor do campo e converta-o para uma string
        field_value = feature.GetField(i)
        if field_value is None:
            field_value = ''
        else:
            field_value = str(field_value)

        print("| {:<14} | {:<14} | {}".format(
            fieldName, fieldType, field_value))

    # Print da geometria
    try:
        geometry_wkt = feature.GetGeometryRef().ExportToWkt()
        print("| Geometria: ", geometry_wkt)
    except:
        pass

    print(f"| Número de feições na camada: {layer.GetFeatureCount()}\n")


def export_geojson(out_name, layer_name, output_dir):
    driver_export = ogr.GetDriverByName('GeoJSON')
    if os.path.exists(f'{output_dir}{out_name}.geojson'):
        driver_export.DeleteDataSource(f'{output_dir}{out_name}.geojson')

    ds_export = driver_export.CreateDataSource(f'{output_dir}{out_name}.geojson')
    ds_export.CopyLayer(layer_name, f'{out_name}')
    ds_export = None


# Recuperar o layer demandas
demandas = Demanda(ds_associado)

# Recuperar o layer_lote
layer_lotes = ds_associado.GetLayerByName('layer_lotes')

# realiza a verificação da existência da camada layer_lotes
if layer_lotes:
    demandas_street_code = demandas.associa_streetcode_demanda(layer_lotes)
else:
    # caso não exista, será utlizada a testada como base
    testada = Testada(ds_associado).gerar_testadas()
    demandas_street_code = demandas.associa_streetcode_demanda(testada)

lista_street_code = demandas.recupera_streetcodes_com_demanda()

arruamento = Arruamento(ds_associado)

arruamento_recortado_lyr = arruamento.cria_arruamento_recortado()

# ordenar arruamentos a partir do comprimento
arruamento_ordenado = arruamento.ordena_arruamento_por_comprimento(lista_street_code)

# instancia layer areas_de_caixa (vazio)
areas_caixa = AreaCaixa(ds_associado, distancia_buffer=0)

demandas_ordenadas = None

# percorrer arruamentos um a um
for feature in arruamento_ordenado:
    # ordenar demandas pela distancia do inicio do arruamento

    street_code = feature['StreetCode']

    demandas_ordenadas = demandas.gera_demandas_ordenadas_por_arruamento(street_code)

    # obtem a dist. maxima p/ utilizar na criacao da caixa:
    dist_maxima_arruamento = demandas.get_maior_distancia_arruamento(demandas_ordenadas, street_code)

    dados_pnt_inicial_final = demandas.gera_pnt_inicial_final_id_caixas(demandas_ordenadas, street_code)

    for item in dados_pnt_inicial_final:
        id_caixa = item.get('id_caixa')
        pnt_inicial = item.get('pnt_inicial', 0)
        pnt_final = item.get('pnt_final', 0)

        if pnt_inicial and pnt_final:
            # recortar arruamento para servir de 'linha centro' para a caixa
            arruamento_recortado = arruamento.recorta_arruamento(
                arruamento_recortado_lyr,
                pnt_inicial,
                pnt_final,
                street_code,
                id_caixa
            )
            caixa = areas_caixa.add_area_caixa(id_caixa, dist_maxima_arruamento)

# TODO loop das caixas e atualiza lembrar de criar uma linha

demandas.atualiza_campo_associado()

linhas = demandas.linhas_demandas()

print(linhas)

# -----------------------------------------------------------
areas_caixa = ds_associado.GetLayer('areas_de_caixa')

export_geojson('demandas_ordenadas', demandas_ordenadas, output_dir)
# export_geojson('demandas_ordenadas_sem_caixa', lyr_demandas_ordenadas, output_dir)
export_geojson('arruamento_recortado', arruamento_recortado_lyr, output_dir)
export_geojson('areas_caixa', areas_caixa, output_dir)

end_time = time.time()
total_time = end_time - start_time
print("Tempo total de processamento:", round(total_time, 2), "segundos")
