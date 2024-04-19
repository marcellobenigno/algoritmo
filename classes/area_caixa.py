import math

from osgeo import ogr

ogr.UseExceptions()


class AreaCaixa:
    def __init__(self, datasource_entrada, layer='areas_de_caixa', distancia_buffer=5):
        self.datasource_entrada = datasource_entrada
        self.layer = layer
        self.buffer = distancia_buffer
        self.cria_layer()

    def __str__(self):
        return self.get_layer().GetName()

    def get_layer(self, bbox=None):
        lyr = self.datasource_entrada.GetLayer(self.layer)
        lyr.SetSpatialFilter(None)
        if bbox:
            lyr.SetSpatialFilter(bbox)
        return lyr

    def cria_layer(self):
        layer = self.datasource_entrada.CreateLayer(self.layer, srs=self.get_srs(), geom_type=ogr.wkbPolygon)
        layer.CreateField(ogr.FieldDefn('id_caixa', ogr.OFTString))
        layer.CreateField(ogr.FieldDefn('StreetCode_associado', ogr.OFTInteger))
        layer.CreateField(ogr.FieldDefn('market-index', ogr.OFTReal))
        layer.CreateField(ogr.FieldDefn('dist_max', ogr.OFTReal))
        layer.CreateField(ogr.FieldDefn('ordem', ogr.OFTInteger))
        return layer

    def get_srs(self):
        return self.datasource_entrada.GetLayer().GetSpatialRef()

    def check_arruamento_intercepta_caixa(self, pol_caixa_wkt):
        sql = f'''
                SELECT  "StreetCode_associado", geometry
                FROM areas_de_caixa
                WHERE ST_Intersects(geometry, ST_GeomFromText('{pol_caixa_wkt}'))
              '''
        query = self.datasource_entrada.ExecuteSQL(sql, dialect="SQLite")
        qtde_caixas_interceptadas = query.GetFeatureCount()

        self.datasource_entrada.ReleaseResultSet(query)

        return qtde_caixas_interceptadas

    def calcula_market_index(self):
        sql = '''
        SELECT a.id_caixa, a.geometry, SUM(b."market-index") AS soma_market_index
        FROM  areas_de_caixa a, layer_demandas_ordenadas b
        WHERE ST_Contains(a.geometry, b.geometry)
        GROUP BY a.id_caixa, a.geometry
        '''
        query = self.datasource_entrada.ExecuteSQL(sql, dialect="SQLite")
        areas_de_caixas = self.datasource_entrada.GetLayer('areas_de_caixa')
        areas_de_caixas.StartTransaction()

        dados = {}
        for row in query:
            dados[row['id_caixa']] = row['soma_market_index']

        for feature in areas_de_caixas:
            if feature['id_caixa'] in dados:
                feature.SetField('market-index', dados[feature['id_caixa']])
                areas_de_caixas.SetFeature(feature)

        areas_de_caixas.CommitTransaction()
        self.datasource_entrada.ReleaseResultSet(query)

        return areas_de_caixas

    def caixa_sql_query(self, dist_maxima_arruamento, where_clause='1=1'):
        sql = f'''
                    SELECT 
                            BufferOptions_SetEndCapStyle('FLAT'),
                            BufferOptions_SetJoinStyle('MITRE'),
                            BufferOptions_SetMitreLimit(2.0),
                        tmp."StreetCode_associado", 
                        ST_Buffer(tmp.geometry_buffer, 1.5) AS geometry 
                    FROM        
                        (SELECT
                            BufferOptions_SetEndCapStyle('FLAT'),
                            BufferOptions_SetJoinStyle('MITRE'),
                            BufferOptions_SetMitreLimit(2.5),
                            "StreetCode" AS 'StreetCode_associado',
                            ST_Buffer(geometry, {dist_maxima_arruamento}) AS geometry_buffer
                        FROM lyr_arruamento_recortado
                        WHERE {where_clause}
                        ) tmp
                    '''
        return sql

    def add_area_caixa(self, id_caixa, dist_maxima_arruamento):
        caixa_criada = False
        lyr_arruamento_recortado = self.datasource_entrada.GetLayer('lyr_arruamento_recortado')
        lyr_arruamento_recortado.SetAttributeFilter(f"id_caixa = '{id_caixa}'")
        sql = self.caixa_sql_query(dist_maxima_arruamento)
        query = self.datasource_entrada.ExecuteSQL(sql, dialect="SQLite")

        self.get_layer().StartTransaction()

        for row in query:
            if row['geometry']:
                feature = ogr.Feature(self.get_layer().GetLayerDefn())
                feature.SetGeometry(row['geometry'])
                pol_caixa_wkt = feature.GetGeometryRef().ExportToWkt()

                if not self.check_arruamento_intercepta_caixa(pol_caixa_wkt):
                    feature.SetField('id_caixa', id_caixa)
                    feature.SetField('StreetCode_associado', row['StreetCode_associado'])
                    feature.SetField('market-index', None)
                    feature.SetField('dist_max', dist_maxima_arruamento)
                    feature.SetField('ordem', 1)
                    self.get_layer().SetFeature(feature)
                    caixa_criada = True

        self.get_layer().CommitTransaction()
        self.datasource_entrada.ReleaseResultSet(query)
        lyr_arruamento_recortado.SetAttributeFilter(None)

        return caixa_criada

    def add_area_caixa_secundaria(self, id_caixa, dist_maxima_arruamento):
        caixa_criada = False
        lyr_arruamento_recortado = self.datasource_entrada.GetLayer('lyr_arruamento_recortado')
        lyr_arruamento_recortado.SetAttributeFilter(f"id_caixa = '{id_caixa}'")

        if lyr_arruamento_recortado.GetFeatureCount():
            sql = self.caixa_sql_query(
                dist_maxima_arruamento=dist_maxima_arruamento,
                where_clause=f"id_caixa = '{id_caixa}'")
            query = self.datasource_entrada.ExecuteSQL(sql, dialect="SQLite")

            self.get_layer().StartTransaction()

            for row in query:
                feature = ogr.Feature(self.get_layer().GetLayerDefn())
                if not row['geometry'].IsEmpty():
                    feature.SetGeometry(row['geometry'])
                    pol_caixa = feature.GetGeometryRef()

                    # ver se é necessário continuar aqui depois da criacao do subtrai_area_sem_demanda
                    for feat_caixa in self.get_layer(pol_caixa.Buffer(30)):
                        geom_caixa = feat_caixa.GetGeometryRef()
                        if geom_caixa.Intersects(pol_caixa):
                            pol_caixa = pol_caixa.Difference(geom_caixa)
                        else:
                            pass

                    feature.SetGeometry(pol_caixa)
                    feature.SetField('id_caixa', id_caixa)
                    feature.SetField('StreetCode_associado', row['StreetCode_associado'])
                    feature.SetField('market-index', None)
                    feature.SetField('dist_max', dist_maxima_arruamento)
                    feature.SetField('ordem', 2)
                    self.get_layer().SetFeature(feature)
                    caixa_criada = True

            self.get_layer().CommitTransaction()
            self.datasource_entrada.ReleaseResultSet(query)
        lyr_arruamento_recortado.SetAttributeFilter(None)

        return caixa_criada

    def remove_sobreposicoes(self):
        areas_1 = self.datasource_entrada.CopyLayer(self.get_layer(), 'areas_1')
        areas_1.SetAttributeFilter("ordem = 1")
        areas_2 = self.datasource_entrada.CopyLayer(self.get_layer(), 'areas_2')
        areas_2.SetAttributeFilter("ordem = 2")

        intersection_list = []
        for area_1 in areas_1:
            geom_1 = area_1.GetGeometryRef()
            for area_2 in areas_2:
                geom_2 = area_2.GetGeometryRef()
                if geom_1.Intersects(geom_2):
                    geom_result = geom_1.Intersection(geom_2)
                    intersection_list.append((area_2['id_caixa'], geom_result))

        for area_2 in areas_2:
            geom_2 = area_2.GetGeometryRef()
            for id_caixa, geom_inter in intersection_list:
                if geom_inter.Intersects(geom_inter) and area_2['id_caixa'] == id_caixa:
                    geom_result = geom_2.Difference(geom_inter)
                    print(geom_result.ExportToWkt())

        self.datasource_entrada.DeleteLayer('areas_1')
        self.datasource_entrada.DeleteLayer('areas_2')

    def absorve_demandas_sem_caixa(self):
        lyr_demandas = self.datasource_entrada.GetLayer('layer_demandas_ordenadas')
        lyr_demandas.SetAttributeFilter('associado = 0')

        caixas_delete_list = []
        caixa_mais_proxima = None  # Initialize caixa_mais_proxima outside the loop

        for demanda in lyr_demandas:
            geom_demanda = demanda.GetGeometryRef()
            areas_caixa = self.get_layer(geom_demanda.Buffer(20))
            areas_caixa.SetAttributeFilter(f''' StreetCode_associado = {demanda['StreetCode']} ''')
            for caixa in areas_caixa:
                geom_caixa = caixa.GetGeometryRef()

                min_dist = float('inf')
                # caixa_mais_proxima = None  # Removed this line as it resets the variable in each iteration

                distance = geom_demanda.Distance(geom_caixa)

                if distance < min_dist:
                    min_dist = distance
                    caixa_mais_proxima = caixa
                    fid = caixa_mais_proxima.GetFID()
                    caixas_delete_list.append(fid)

        if caixa_mais_proxima:
            arruamento_recortado = self.datasource_entrada.GetLayer('lyr_arruamento_recortado')

            arruamento_recortado.SetAttributeFilter(f''' id_caixa =  '{caixa_mais_proxima["id_caixa"]}' ''')
            feat = arruamento_recortado.GetNextFeature()
            line = feat.GetGeometryRef()

            index_ultimo_ponto = line.GetPointCount() - 1
            ultimo_ponto_x, ultimo_ponto_y, _ = line.GetPoint(index_ultimo_ponto)

            # Calcula a direcao da linha a partir do penultimo ponto
            penultimo_ponto_x, penultimo_ponto_y, _ = line.GetPoint(index_ultimo_ponto - 1)
            direcao_x = ultimo_ponto_x - penultimo_ponto_x
            direcao_y = ultimo_ponto_y - penultimo_ponto_y

            # Normaliza a direcao
            length = math.sqrt(direcao_x ** 2 + direcao_y ** 2)
            direcao_x /= length
            direcao_y /= length

            # aumenta o comprimento da linha com a dist. min + 0.2m
            novo_vertice_x = ultimo_ponto_x + (min_dist + 0.2) * direcao_x
            novo_vertice_y = ultimo_ponto_y + (min_dist + 0.2) * direcao_y

            # adiciona o vertice
            line.AddPoint(novo_vertice_x, novo_vertice_y)
            line.FlattenTo2D()

            # atualiza a geometria da linha
            if line:
                feat.SetGeometry(line)
                arruamento_recortado.SetFeature(feat)
                id_caixa = caixa_mais_proxima['id_caixa']
                dist_maxima_arruamento = caixa_mais_proxima['dist_max']
                self.add_area_caixa_secundaria(id_caixa, dist_maxima_arruamento)
                caixas_delete_list.append(caixa_mais_proxima.GetFID())

        lyr = self.get_layer(bbox=None)
        lyr.SetAttributeFilter(None)

        lyr_demandas.SetSpatialFilter(None)
        lyr_demandas.SetAttributeFilter(None)

        for fid in list(set(caixas_delete_list)):
            self.get_layer().DeleteFeature(fid)

    def identifica_caixas_m8(self):
        self.get_layer().SetAttributeFilter(' "market-index" > 8')
        id_caixas_list = [(caixa['id_caixa'], caixa['market-index']) for caixa in self.get_layer()]
        self.get_layer().SetAttributeFilter(None)
        return id_caixas_list

    def get_parametros_caixas_m8(self):
        id_caixas_maiores_8 = self.identifica_caixas_m8()
        demandas_ordenadas = self.datasource_entrada.GetLayer('layer_demandas_ordenadas')
        street_code_caixa_demandas = []

        for id_caixa, caixa_market_index in id_caixas_maiores_8:
            street_code = id_caixa.split('.')[0]
            demandas_ordenadas.SetAttributeFilter(f"id_caixa = '{id_caixa}'")
            lista_demandas_caixa_1 = []
            lista_demandas_caixa_2 = []
            pontos_caixa_1 = []
            pontos_caixa_2 = []
            acum = 0

            for demanda in demandas_ordenadas:
                geom = demanda.GetGeometryRef()
                acum += demanda['market-index']
                if acum <= caixa_market_index / 2:
                    lista_demandas_caixa_1.append(demanda['id'])
                    pontos_caixa_1.append(geom.ExportToWkt())
                else:
                    lista_demandas_caixa_2.append(demanda['id'])
                    pontos_caixa_2.append(geom.ExportToWkt())


            if len(pontos_caixa_1) and len(pontos_caixa_2):
                caixa_1 = {
                    'street_code': street_code,
                    'id_caixa_antigo': f'{id_caixa}',
                    'id_caixa': f'{id_caixa}.1',
                    'demandas': lista_demandas_caixa_1,
                    'ponto_inicial': pontos_caixa_1[0],
                    'ponto_final': pontos_caixa_1[-1]
                }
                caixa_2 = {
                    'street_code': street_code,
                    'id_caixa_antigo': f'{id_caixa}',
                    'id_caixa': f'{id_caixa}.2',
                    'demandas': lista_demandas_caixa_2,
                    'ponto_inicial': pontos_caixa_2[0],
                    'ponto_final': pontos_caixa_2[-1]
                }

                street_code_caixa_demandas.append(caixa_1)
                street_code_caixa_demandas.append(caixa_2)

        demandas_ordenadas.SetAttributeFilter(None)

        return street_code_caixa_demandas

    # def get_parametros_caixas_m8(self):
    #     street_code_caixa_demandas = []
    #     query = None
    #     id_caixas_list = self.identifica_caixas_m8()
    #     lyr_area_caixa = self.datasource_entrada.GetLayer('areas_de_caixa')
    #     self.datasource_entrada.CopyLayer(lyr_area_caixa, 'lyr_area_caixa_tmp')
    #
    #     lyr_demandas = self.datasource_entrada.GetLayer('layer_demandas_ordenadas')
    #     self.datasource_entrada.CopyLayer(lyr_demandas, 'layer_demandas_ordenadas_tmp')
    #
    #     for id_caixa, caixa_market_index in id_caixas_list:
    #
    #         sql = f'''
    #         SELECT b.id, b.id_caixa, b."market-index" AS demanda_market_index, c."StreetCode",
    #                a."market-index" AS caixa_market_index, ST_AsText(b.geometry) AS geom_wkt
    #         FROM lyr_area_caixa_tmp a, layer_demandas_ordenadas_tmp b, lyr_arruamento_recortado c
    #         WHERE ST_Contains(a.geometry, b.geometry) AND ST_Contains(a.geometry, c.geometry)
    #         AND a.id_caixa = '{id_caixa}' AND b.id_caixa = '{id_caixa}'
    #         ORDER BY a.id_caixa, ST_Distance(b.geometry, ST_StartPoint(c.geometry))
    #         '''
    #         query = self.datasource_entrada.ExecuteSQL(sql, dialect="SQLite")
    #         acum = 0
    #         lista_demandas_caixa_1 = []
    #         lista_demandas_caixa_2 = []
    #
    #         for row in query:
    #
    #             acum += row['demanda_market_index']
    #             if acum <= caixa_market_index / 2:
    #                 lista_demandas_caixa_1.append(row['id'])
    #             else:
    #                 lista_demandas_caixa_2.append(row['id'])
    #
    #         print(id_caixa, lista_demandas_caixa_1, lista_demandas_caixa_2)
    #
    #     self.datasource_entrada.ReleaseResultSet(query)
    #     self.datasource_entrada.DeleteLayer('lyr_area_caixa_tmp')
    #     self.datasource_entrada.DeleteLayer('layer_demandas_ordenadas_tmp')
    #
    #     print(street_code_caixa_demandas)
    #
    #     return street_code_caixa_demandas

    def apaga_caixas_8m(self):
        id_caixas_8m = [id_caixa[0] for id_caixa in self.identifica_caixas_m8()]
        demandas = self.datasource_entrada.GetLayer('layer_demandas_ordenadas')
        demandas.StartTransaction()
        for id_caixa in id_caixas_8m:
            demandas.SetAttributeFilter(f"id_caixa = '{id_caixa}'")
            for demanda in demandas:
                demanda.SetField('id_caixa', None)
                demandas.SetFeature(demanda)

        demandas.CommitTransaction()
        demandas.SetAttributeFilter(None)

        for id_caixa in id_caixas_8m:
            self.get_layer().SetAttributeFilter(f"id_caixa ='{id_caixa}'")
            self.get_layer().StartTransaction()

            for feature in self.get_layer():
                self.get_layer().DeleteFeature(feature.GetFID())

            self.get_layer().CommitTransaction()
            self.get_layer().SetAttributeFilter(None)
