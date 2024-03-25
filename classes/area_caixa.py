import os

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
        return layer

    def get_srs(self):
        return self.datasource_entrada.GetLayer().GetSpatialRef()

    def export_geojson(self, out_name, layer_name, output_dir):
        driver_export = ogr.GetDriverByName('GeoJSON')
        if os.path.exists(f'{output_dir}{out_name}.geojson'):
            driver_export.DeleteDataSource(f'{output_dir}{out_name}.geojson')

        ds_export = driver_export.CreateDataSource(f'{output_dir}{out_name}.geojson')
        ds_export.CopyLayer(layer_name, f'{out_name}')
        ds_export = None

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
        SELECT a.id_caixa, SUM(b."market-index") AS soma_market_index
        FROM  areas_de_caixa a, layer_demandas_ordenadas b
        WHERE ST_Intersects(a.geometry, b.geometry)
        GROUP BY a.id_caixa
        '''

        query = self.datasource_entrada.ExecuteSQL(sql, dialect="SQLite")

        areas_de_caixas = self.datasource_entrada.GetLayer('areas_de_caixa')

        areas_de_caixas.StartTransaction()
        feature = ogr.Feature(areas_de_caixas.GetLayerDefn())

        for row in query:
            for feature in areas_de_caixas:
                if feature['id_caixa'] == row['id_caixa']:
                    feature.SetField('market-index', row['soma_market_index'])
                    areas_de_caixas.SetFeature(feature)

        areas_de_caixas.CommitTransaction()
        self.datasource_entrada.ReleaseResultSet(query)

        return areas_de_caixas

    def caixa_sql_query(self, dist_maxima_arruamento, where_clause='1=1'):
        sql = f'''
                    SELECT 
                            BufferOptions_SetEndCapStyle('FLAT'),
                            BufferOptions_SetJoinStyle('MITRE'),
                            BufferOptions_SetMitreLimit(2.5),
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
            feature = ogr.Feature(self.get_layer().GetLayerDefn())
            feature.SetGeometry(row['geometry'])
            pol_caixa_wkt = feature.GetGeometryRef().ExportToWkt()

            if not self.check_arruamento_intercepta_caixa(pol_caixa_wkt):
                feature.SetField('id_caixa', id_caixa)
                feature.SetField('StreetCode_associado', row['StreetCode_associado'])
                feature.SetField('market-index', None)
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
                feature.SetGeometry(row['geometry'])
                pol_caixa = feature.GetGeometryRef()

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
                self.get_layer().SetFeature(feature)
                caixa_criada = True

            self.get_layer().CommitTransaction()
            self.datasource_entrada.ReleaseResultSet(query)
        lyr_arruamento_recortado.SetAttributeFilter(None)

        return caixa_criada
