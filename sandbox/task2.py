# Criar caixas a partir da rua mais longa.

# Recuperar Layer arruamento e demandas
# ordenar arruamentos a partir do comprimento
# percorrer arruamentos um a um
#     - filtrar demandas a partir do streetcode.
#     - ordenar demandas pela distancia do inicio do arruamento
#     - identificar as demandas que serão absorvidas pela caixa até o limiar (8)
#     - recortar arruamento para servir de 'linha centro' para a caixa.
#     - verificar se esse arruamento intercepta outra caixa.

#         Caso não intercepte.
#             - identificar a maior distancia entre as demandas selecionadas e a linha centro.*
#             - realizar o buffer (Flat|Mitre) com o valor da distancia das demandas + 0.5m *

#         Caso intercepte.
#             - identificar demandas que não são interceptadas pela caixa
#             - identificar distancias entre as demandas selecionadas e a linha centro.*
#             - realizar o buffer (Flat|Mitre) com o valor da distancia das demandas + 0.5m*


from osgeo import ogr

ogr.UseExceptions()


def poligoniza_areas_de_caixa(self, datasource_in, lyr_demandas, nome_saida='areas_de_caixa', parametro_buffer=5):
    print("\nExecutando poligoniza_areas_de_caixa")

    layer_demandas = datasource_in.GetLayer(lyr_demandas)
    layer_demandas_temp = datasource_in.CopyLayer(layer_demandas, layer_demandas.GetName() + "_temp")

    # Obter SRC de saída
    srs_saida = datasource_in.GetLayer().GetSpatialRef()

    # Layer que receberá os arruamentos recortados (a base para geração dos buffers de área de caixa)
    lyr_arruamentos_recortados = datasource_in.CreateLayer("layer_arruamentos_recortados",
                                                           geom_type=ogr.wkbLineString, srs=srs_saida)

    lyr_arruamentos_recortados.CreateField(
        ogr.FieldDefn("StreetCode", ogr.OFTInteger))
    lyr_arruamentos_recortados.CreateField(
        ogr.FieldDefn("id_poste_associado", ogr.OFTInteger))
    lyr_arruamentos_recortados.CreateField(
        ogr.FieldDefn("distancia_buffer", ogr.OFTReal))
    lyr_arruamentos_recortados.CreateField(
        ogr.FieldDefn("market-index", ogr.OFTReal))

    # Agrupar as demandas por streetcode
    lista_streetcodes = []
    for fc_1 in layer_demandas_temp:
        lista_streetcodes.append(fc_1.GetField("Streetcode_associado"))
    lista_streetcodes = list(set(lista_streetcodes))

    for streetcode in lista_streetcodes:
        # Filtrar as demandas por streetcode
        layer_demandas_filtradas = datasource_in.ExecuteSQL(
            f'''SELECT * FROM {layer_demandas_temp.GetName()} WHERE StreetCode_associado = {streetcode}''',
            dialect="SQLite")

        # Para cada conjunto de demandas agrupadas por streetcode, sub-agrupar por poste
        lista_id_postes = []
        for fc_2 in layer_demandas_filtradas:
            if fc_2.GetField("id_poste_associado"):
                lista_id_postes.append(fc_2.GetField("id_poste_associado"))
        lista_id_postes = list(set(lista_id_postes))

        datasource_in.ReleaseResultSet(layer_demandas_filtradas)
        # print("\nLista id postes: ", lista_id_postes)

        # Filtrar as demandas por streetcode e id_poste
        for id_poste_associado in sorted(lista_id_postes):
            if id_poste_associado is not None:
                layer_demandas_filtradas = datasource_in.ExecuteSQL(
                    f'''SELECT * FROM {layer_demandas_temp.GetName()} WHERE "StreetCode_associado" = {streetcode} AND "id_poste_associado" = {id_poste_associado}''',
                    dialect="SQLite")

                # Obter valor de market-index
                market_index = 0
                for feature in layer_demandas_filtradas:
                    market_index += feature.GetField("market-index")

                geom_demanda_inicial, geom_demanda_final = self.ordenar_demandas(datasource_in, 'layer_arruamento',
                                                                                 layer_demandas_temp.GetName(),
                                                                                 id_poste_associado, streetcode)

                # Adicionado para poder debugar e checar a consulta
                sql = f'''
                        SELECT a.StreetCode,
                                ST_Line_Substring(
                                    a.GEOMETRY,
                                    ST_Line_Locate_Point(
                                        a.GEOMETRY,
                                        ST_ClosestPoint(
                                            a.GEOMETRY,
                                            ST_GeomFromText('{geom_demanda_inicial}')
                                        )
                                    ),
                                    ST_Line_Locate_Point(
                                        a.GEOMETRY,
                                        ST_ClosestPoint(
                                            a.GEOMETRY,
                                            ST_GeomFromText('{geom_demanda_final}')
                                        )
                                    )
                                ) AS GEOMETRY
                    FROM 
                        layer_arruamento a 
                    WHERE StreetCode = {streetcode}

                '''

                layer_arruamento_recortado = datasource_in.ExecuteSQL(
                    sql, dialect="SQLite")

                # Isola a geometria do arruamento recortado (foi feito um for mas só tem uma feature)
                for feature in layer_arruamento_recortado:
                    geom_arruamento_recortado = feature.GetGeometryRef()

                if geom_arruamento_recortado is not None and geom_arruamento_recortado.Length() > 1:
                    # Qual é a demanda com maior distância do arruamento recortado? Será usada para buffer
                    # print("Vai identificar a maior distância entre o arruamento e uma demanda associada")
                    distancia_buffer = 0
                    for fc_3 in layer_demandas_filtradas:
                        if fc_3.GetGeometryRef().Distance(geom_arruamento_recortado) > distancia_buffer:
                            distancia_buffer = fc_3.GetGeometryRef().Distance(geom_arruamento_recortado)

                    # print(f"Distância do buffer: {distancia_buffer + parametro_buffer}")

                    # Alimentando dataset de saída (se o arruamento for menor do que 180m)
                    # print("Vai alimentar dataset de saída")
                    feature_saida = ogr.Feature(
                        lyr_arruamentos_recortados.GetLayerDefn())
                    feature_saida.SetGeometry(geom_arruamento_recortado)
                    feature_saida.SetField("StreetCode", streetcode)
                    feature_saida.SetField(
                        "id_poste_associado", id_poste_associado)
                    feature_saida.SetField(
                        "distancia_buffer", distancia_buffer + parametro_buffer)
                    feature_saida.SetField(
                        "market-index", market_index)
                    lyr_arruamentos_recortados.CreateFeature(feature_saida)
                    # print(f"Comprimento do arruamento recortado: {geom_arruamento_recortado.Length()}.")

                datasource_in.ReleaseResultSet(layer_demandas_filtradas)
                datasource_in.ReleaseResultSet(layer_arruamento_recortado)

    layer_areas_de_caixa = datasource_in.ExecuteSQL(
        f'''
        SELECT 
            BufferOptions_SetJoinStyle('MITRE'), 
            b."StreetCode_associado", 
            b."id_poste_associado",
            b."market-index", 
            ST_Buffer(b.GEOMETRY, 1.5) AS GEOMETRY 
        FROM
            (SELECT 
                BufferOptions_SetEndCapStyle('FLAT'),
                BufferOptions_SetJoinStyle('MITRE'),
                BufferOptions_SetMitreLimit(2.5),
                a.StreetCode as "StreetCode_associado",
                a.id_poste_associado,
                a."market-index",
                ST_Buffer(ST_LineMerge(a.GEOMETRY), distancia_buffer) GEOMETRY

            FROM layer_arruamentos_recortados a
            GROUP BY id_poste_associado, StreetCode) b
        ''', dialect="SQLite")

    # Incluir layers áreas de caixa e demandas com id_poligono no datasource de saída
    datasource_in.CopyLayer(layer_areas_de_caixa, nome_saida)
    datasource_in.DeleteLayer(layer_demandas_temp.GetName())
    datasource_in.DeleteLayer(lyr_arruamentos_recortados.GetName())
    datasource_in.ReleaseResultSet(layer_areas_de_caixa)

    # Explodir multipolygons
    self.explode_multipolygons(datasource_in, str_lyr_caixas=nome_saida)

    return datasource_in
