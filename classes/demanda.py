from collections import defaultdict

from osgeo import ogr

ogr.UseExceptions()


class Demanda:
    def __init__(self, datasource_entrada, layer='layer_demandas'):
        self.datasource_entrada = datasource_entrada
        self.layer = layer
        self.demandas_ordenadas = self.cria_demandas_ordenadas_por_arruamento()
        self.cria_layer_linhas_demandas()

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
        return list(set([feature['StreetCode'] for feature in self.get_layer() if feature['StreetCode'] is not None]))

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

    def get_demandas_ordenadas_por_arruamento(self, i, streetcode):
        sql = f'''    
                 SELECT
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
                feature.SetField('id', i)
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
                i += 1

            self.datasource_entrada.ReleaseResultSet(query)

            return self.datasource_entrada.GetLayer('layer_demandas_ordenadas')

    def get_maior_distancia_arruamento(self, id_caixa, lista_demandas=None):
        max_dist_arruamento = 0
        lyr_demandas_ordenadas = self.datasource_entrada.GetLayer('layer_demandas_ordenadas')
        self.datasource_entrada.CopyLayer(lyr_demandas_ordenadas, 'layer_demandas_ordenadas_tmp')

        sql = f'''
                    SELECT max(dist_arruamento) AS max_dist_arruamento
                    FROM layer_demandas_ordenadas_tmp
                    WHERE "id_caixa" = '{id_caixa}'
                '''
        if lista_demandas:
            lista_demandas_str = [str(demanda) for demanda in lista_demandas]
            sql += f' AND id IN ({", ".join(lista_demandas_str)})'

        query = self.datasource_entrada.ExecuteSQL(sql, dialect="SQLite")

        for row in query:
            max_dist_arruamento = row['max_dist_arruamento']

        self.datasource_entrada.ReleaseResultSet(query)
        self.datasource_entrada.DeleteLayer('layer_demandas_ordenadas_tmp')

        return max_dist_arruamento

    def get_pnt_inicial_final_id_caixas(self, lyr_demanda_ordenada, street_code):
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
                id_caixa = feat_caixa['id_caixa']
                if not geom_demandas.Disjoint(geom_caixa):
                    is_disjoint = 1
                    id_caixa = feat_caixa['id_caixa']
                    feat_demandas.SetField('id_caixa', id_caixa)
                    break
            feat_demandas.SetField('associado', is_disjoint)
            lyr_demandas_ordenadas.SetFeature(feat_demandas)

        lyr_demandas_ordenadas.CommitTransaction()
        return lyr_demandas_ordenadas

    def cria_layer_linhas_demandas(self):
        lyr_linhas_demandas = self.datasource_entrada.CreateLayer(
            'layer_linhas_demandas', srs=self.get_srs(), geom_type=ogr.wkbLineString
        )
        lyr_linhas_demandas.CreateField(ogr.FieldDefn('id_caixa', ogr.OFTString))

        return lyr_linhas_demandas

    def atualiza_id_caixa_demandas(self):
        lyr_demandas = self.datasource_entrada.GetLayer('layer_demandas_ordenadas')
        areas_caixa = self.datasource_entrada.GetLayer('areas_de_caixa')
        id_caixa_list = list(set([feat['id_caixa'] for feat in lyr_demandas]))

        lyr_demandas.StartTransaction()

        for id_caixa in id_caixa_list:
            lyr_demandas.SetAttributeFilter(f"id_caixa = '{id_caixa}' AND associado = 0")
            lista_fids = [fc.GetFID() for fc in lyr_demandas]
            linestring = ogr.Geometry(ogr.wkbLineString)

            for i, val in enumerate(lista_fids):
                if i + 1 < len(lista_fids):
                    feature_current = lyr_demandas.GetFeature(lista_fids[i])
                    feature_next = lyr_demandas.GetFeature(lista_fids[i + 1])

                    geom_current = feature_current.GetGeometryRef()
                    geom_next = feature_next.GetGeometryRef()

                    linestring.AddPoint(geom_current.GetX(), geom_current.GetY())
                    linestring.AddPoint(geom_next.GetX(), geom_next.GetY())
                    linestring.FlattenTo2D()

                    areas_caixa.SetSpatialFilter(linestring)

                    if areas_caixa.GetFeatureCount() > 0:

                        for caixa in areas_caixa:
                            caixa_geom = caixa.GetGeometryRef()
                            if caixa_geom.Intersects(linestring):
                                # demandas antes da caixa interceptada:
                                lyr_demandas.SetAttributeFilter(
                                    f"id_caixa = '{id_caixa}' AND associado = 0 AND id <= {feature_current['id']}"
                                )
                                for demanda in lyr_demandas:
                                    demanda.SetField("id_caixa", f'{id_caixa}.1')
                                    lyr_demandas.SetFeature(demanda)

                                # demandas depois da caixa interceptada:
                                lyr_demandas.SetAttributeFilter(
                                    f"id_caixa = '{id_caixa}' AND associado = 0 AND id > {feature_current['id']}"
                                )
                                for demanda in lyr_demandas:
                                    demanda.SetField("id_caixa", f'{id_caixa}.2')
                                    lyr_demandas.SetFeature(demanda)
                        break

        lyr_demandas.CommitTransaction()
        lyr_demandas.SetAttributeFilter(None)
        areas_caixa.SetSpatialFilter(None)

    def atualiza_campo_id_caixa(self):
        lyr_demandas_ordenadas = self.datasource_entrada.GetLayer('layer_demandas_ordenadas')
        lyr_demandas_ordenadas.SetAttributeFilter('associado = 0')

        lyr_linhas = self.datasource_entrada.GetLayer('layer_linhas_demandas')

        lyr_demandas_ordenadas.StartTransaction()

        for linha in lyr_linhas:
            linha_geom = linha.GetGeometryRef()
            for demanda in lyr_demandas_ordenadas:
                demanda_geom = demanda.GetGeometryRef()
                if demanda_geom.Buffer(0.1).Intersects(linha_geom):
                    demanda.SetField('id_caixa', linha.GetField('id_caixa'))
                    lyr_demandas_ordenadas.SetFeature(demanda)

        lyr_demandas_ordenadas.CommitTransaction()
        lyr_demandas_ordenadas.SetAttributeFilter(None)

        return lyr_demandas_ordenadas

    def modifica_id_caixa_maior_8(self, caixa):
        lyr_demandas_ordenadas = self.datasource_entrada.GetLayer('layer_demandas_ordenadas')
        lyr_demandas_ordenadas.SetAttributeFilter(f"id in {tuple(caixa['demandas'])}")
        for feature in lyr_demandas_ordenadas:
            feature.SetField("id_caixa", caixa['id_caixa'])
            lyr_demandas_ordenadas.SetFeature(feature)

        lyr_demandas_ordenadas.CommitTransaction()
        lyr_demandas_ordenadas.SetAttributeFilter(None)


