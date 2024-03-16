from collections import defaultdict

from osgeo import ogr

ogr.UseExceptions()


class Demanda:
    def __init__(self, datasource_entrada, layer='layer_demandas'):
        self.datasource_entrada = datasource_entrada
        self.layer = layer
        self.demandas_ordenadas = self.cria_demandas_ordenadas_por_arruamento()

    def __str__(self):
        return self.get_layer().GetName()

    def get_layer(self):
        return self.datasource_entrada.GetLayer(self.layer)

    def get_srs(self):
        return self.get_layer().GetSpatialRef()

    def associa_streetcode_demanda(self, layer_com_street_code):
        self.get_layer().CreateField(ogr.FieldDefn('StreetCode', ogr.OFTInteger))
        lyr_com_street_code = self.datasource_entrada.GetLayer(layer_com_street_code.GetName())

        lyr_alvo_index = self.get_indice_espacial(lyr_com_street_code)

        for feat in self.get_layer():
            nearest_feature_fid = self.encontra_feicao_mais_proxima(feat, lyr_alvo_index)
            if nearest_feature_fid is not None:
                nearest_feature = lyr_com_street_code.GetFeature(nearest_feature_fid)
                street_code = nearest_feature.GetField('StreetCode')
                feat.SetField('StreetCode', street_code)
                self.get_layer().SetFeature(feat)

        return self.get_layer()

    def recupera_streetcodes_com_demanda(self):
        return list(set([feature['StreetCode'] for feature in self.get_layer()]))

    def get_indice_espacial(self, layer_com_street_code):
        spatial_index = {}
        for feature in layer_com_street_code:
            spatial_index[feature.GetFID()] = (feature.GetGeometryRef().GetX(), feature.GetGeometryRef().GetY())
        return spatial_index

    def encontra_feicao_mais_proxima(self, feature, spatial_index):
        min_dist = float('inf')
        nearest_fid = None
        feature_geometry = feature.GetGeometryRef()
        feature_point = (feature_geometry.GetX(), feature_geometry.GetY())

        for fid, point in spatial_index.items():
            distance = ((point[0] - feature_point[0]) ** 2 + (point[1] - feature_point[1]) ** 2) ** 0.5
            if distance < min_dist:
                nearest_fid = fid
                min_dist = distance

        return nearest_fid

    def cria_demandas_ordenadas_por_arruamento(self):
        layer = self.datasource_entrada.CreateLayer(
            'layer_demandas_ordenadas', srs=self.get_srs(), geom_type=ogr.wkbPoint
        )
        layer.CreateField(ogr.FieldDefn('id', ogr.OFTInteger))
        layer.CreateField(ogr.FieldDefn('id_demanda', ogr.OFTInteger))
        layer.CreateField(ogr.FieldDefn('StreetCode', ogr.OFTInteger))
        layer.CreateField(ogr.FieldDefn('market-index', ogr.OFTReal))
        layer.CreateField(ogr.FieldDefn('dist_arruamento', ogr.OFTReal))
        layer.CreateField(ogr.FieldDefn('id_caixa', ogr.OFTString))
        layer.CreateField(ogr.FieldDefn('associado', ogr.OFTInteger))
        return layer

    def gera_demandas_ordenadas_por_arruamento(self, streetcode):
        sql = f'''    
                 SELECT
                   ROW_NUMBER() OVER () AS id,
                   id_demanda, 
                   StreetCode, 
                   "market-index",
                   dist_arruamento,
                   geometry
            FROM (
                SELECT
                       d.id_demanda, 
                       d.StreetCode, 
                       d."market-index",
                       ST_Distance(d.geometry, a.geometry) AS dist_arruamento,
                       d.geometry
                FROM layer_arruamento a,
                     layer_demandas d
                WHERE a.StreetCode = {streetcode}
                  AND d.StreetCode = {streetcode}
                ORDER BY st_line_locate_point(a.geometry, st_closestpoint(a.geometry, d.geometry))
            ) AS ordered_data
        '''
        query = self.datasource_entrada.ExecuteSQL(sql, dialect="SQLite")

        id_caixa = 1
        acumulador = 0

        if query:
            for row in query:
                self.get_layer().StartTransaction()
                feature = ogr.Feature(self.demandas_ordenadas.GetLayerDefn())
                feature.SetGeometry(row['geometry'])
                feature.SetField('id', row['id'])
                feature.SetField('id_demanda', row['id_demanda'])
                feature.SetField('StreetCode', row['StreetCode'])
                feature.SetField('market-index', row['market-index'])
                feature.SetField('dist_arruamento', row['dist_arruamento'])
                feature.SetField('associado', 0)

                if acumulador + row['market-index'] <= 8:
                    acumulador += row['market-index']
                else:
                    id_caixa += 1
                    acumulador = row['market-index']

                feature.SetField('id_caixa', f'{row["StreetCode"]}.{id_caixa}')
                self.demandas_ordenadas.SetFeature(feature)
                self.demandas_ordenadas.CommitTransaction()

            self.datasource_entrada.ReleaseResultSet(query)

            return self.datasource_entrada.GetLayer('layer_demandas_ordenadas')

    def get_maior_distancia_arruamento(self, demandas_ordenadas, street_code):

        sql = f'''
                    SELECT max(dist_arruamento) AS max_dist_arruamento
                    FROM {demandas_ordenadas.GetName()}
                    WHERE "StreetCode" = {street_code}
                '''

        query = self.datasource_entrada.ExecuteSQL(sql, dialect="SQLite")

        max_dist_arruamento = 0

        for row in query:
            max_dist_arruamento = row['max_dist_arruamento']

        self.datasource_entrada.ReleaseResultSet(query)

        return max_dist_arruamento

    def gera_pnt_inicial_final_id_caixas(self, lyr_demanda_ordenada, street_code):
        aux = []

        lyr_demanda_ordenada.SetAttributeFilter(f"StreetCode = {street_code}")

        for feature in lyr_demanda_ordenada:
            id = feature['id']
            id_demanda = feature['id_demanda']
            street_code = feature['StreetCode']
            id_caixa = feature['id_caixa']
            geometry = feature.GetGeometryRef().ExportToWkt()
            aux.append({
                'id': id,
                'id_caixa': id_caixa,
                'id_demanda': id_demanda,
                'street_code': street_code,
                'geometry': geometry,
            })

        result = defaultdict(dict)

        for item in aux:
            id_caixa = item['id_caixa']
            geometry = item['geometry']
            if id_caixa not in result:
                result[id_caixa]['id_caixa'] = id_caixa
                result[id_caixa]['pnt_inicial'] = geometry
            else:
                result[id_caixa]['pnt_final'] = geometry

        lyr_demanda_ordenada.SetAttributeFilter(None)

        return list(result.values())

    def atualiza_campo_associado(self):
        lyr_demandas_ordenadas = self.datasource_entrada.GetLayer('layer_demandas_ordenadas')
        lyr_area_caixa = self.datasource_entrada.GetLayer('areas_de_caixa')

        lyr_demandas_ordenadas.StartTransaction()

        for feat_demandas in lyr_demandas_ordenadas:
            geom_demandas = feat_demandas.GetGeometryRef()

            is_disjoint = 0

            for feat_caixa in lyr_area_caixa:
                geom_caixa = feat_caixa.GetGeometryRef()

                if not geom_demandas.Disjoint(geom_caixa):
                    is_disjoint = 1
                    break

            feat_demandas.SetField('associado', is_disjoint)
            lyr_demandas_ordenadas.SetFeature(feat_demandas)

        lyr_demandas_ordenadas.CommitTransaction()

        return lyr_demandas_ordenadas

    def linhas_demandas(self):
        lyr_demandas = self.datasource_entrada.GetLayer('layer_demandas_ordenadas')
        lyr_demandas.SetAttributeFilter("associado = 0 AND id_caixa = '15.1'")

        lyr_area_caixa = self.datasource_entrada.GetLayer('areas_de_caixa')

        lista_fids = [fc.GetFID() for fc in lyr_demandas]

        linestring = ogr.Geometry(ogr.wkbLineString)

        geom_result = None

        for i, val in enumerate(lista_fids):
            if i + 1 < len(lista_fids):
                feature_current = lyr_demandas.GetFeature(lista_fids[i])
                feature_next = lyr_demandas.GetFeature(lista_fids[i + 1])

                geom_current = feature_current.GetGeometryRef()
                geom_next = feature_next.GetGeometryRef()

                linestring.AddPoint(geom_current.GetX(), geom_current.GetY())
                linestring.AddPoint(geom_next.GetX(), geom_next.GetY())
                linestring.FlattenTo2D()

                print(linestring)

        # for feature in lyr_area_caixa:
        #     geom = feature.GetGeometryRef()
        #
        #     geom_result = linestring.Difference(geom)
        #
        #     if geom_result.Disjoint(geom_result)


        # num_features = lyr_area_caixa.GetFeatureCount()

        # for i in range(num_features):
        #     feature = lyr_area_caixa.GetFeature(i)
        #     geom = feature.GetGeometryRef()
        #
        #     simdiff = geom.SymmetricDifference(linestring)
        #     result_feature = ogr.Feature(result_layer.GetLayerDefn())
        #     result_feature.SetGeometry(simdiff)
        #
        #     result_layer.CreateFeature(result_feature)
        #
        #     result_feature.Destroy()
        #     feature.Destroy()

        # sql = f'''SELECT
        #             ST_Intersection(ST_SymDifference(geometry, ST_Buffer(ST_GeomFromText('{linestring}'),0.5)), geometry) AS geometry
        #           FROM  {lyr_area_caixa.GetName()}
        #           WHERE ST_Intersects(geometry, ST_GeomFromText('{linestring}'))
        #    '''

        # query = self.datasource_entrada.ExecuteSQL(sql, dialect="SQLite")

        # linhas = self.datasource_entrada.CopyLayer(query, 'linhas')

        lyr_demandas.SetAttributeFilter(None)
        # self.datasource_entrada.ReleaseResultSet(query)

        return linestring

        # lyr_demandas.SetAttributeFilter(None)
        # return linestring

    # # Create a linestring between the current and next points
    #    line_between_points = ogr.Geometry(ogr.wkbLineString)
    #    line_between_points.AddPoint(geom_current.GetX(), geom_current.GetY())
    #    line_between_points.AddPoint(geom_next.GetX(), geom_next.GetY())
    #
    #    # Add each point from the line_between_points to the main linestring
    #    for j in range(line_between_points.GetPointCount()):
    #        point = line_between_points.GetPoint(j)
    #        linestring.AddPoint(point[0], point[1])
