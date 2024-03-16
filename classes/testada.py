from osgeo import ogr
from shapely.geometry import LineString

ogr.UseExceptions()


class Testada:
    def __init__(self, datasource_entrada, layer_origem='layer_alinhamento_predial'):
        self.datasource_entrada = datasource_entrada
        self.layer_origem = layer_origem

    def get_layer_origem(self):
        return self.datasource_entrada.GetLayer(self.layer_origem)

    def get_srs(self):
        return self.get_layer_origem().GetSpatialRef()

    def gerar_testadas(self):
        """
            Esta função retorna uma camada do tipo ponto, para cada centróide do
            alinhamento predial, contendo o StreetCode do mesmo.

            Args:
                datasource_entrada: DataSource de entrada.

            Returns:
                layer: layer_testada.
        """

        lyr_origem = self.get_layer_origem()
        srs = self.get_srs()
        layer_testada = self.datasource_entrada.CreateLayer('layer_testada', srs, geom_type=ogr.wkbPoint)
        layer_testada.CreateField(ogr.FieldDefn('id_alinhamento_predial', ogr.OFTInteger))
        layer_testada.CreateField(ogr.FieldDefn('id_testada', ogr.OFTInteger))
        layer_testada.CreateField(ogr.FieldDefn('StreetCode', ogr.OFTInteger))

        i = 1
        for feicao in lyr_origem:
            linestring = feicao.GetGeometryRef()
            vertices = linestring.GetPoints()

            segmentos = list(map(LineString, zip(vertices[:-1], vertices[1:])))

            for segmento in segmentos:
                testada_feicao = ogr.Feature(layer_testada.GetLayerDefn())
                testada_feicao.SetGeometry(ogr.CreateGeometryFromWkt(segmento.centroid.wkt))
                testada_feicao.SetField('id_alinhamento_predial', feicao.GetField('id_alinhamento_predial'))
                testada_feicao.SetField('id_testada', i)
                testada_feicao.SetField('StreetCode', None)
                layer_testada.CreateFeature(testada_feicao)
                i += 1

        # Atualiza o campo StreetCode por aproximação do centroide em rel. ao arruamento:
        query = None
        ids_testada_list = [feat.GetField('id_testada') for feat in layer_testada]
        for id_testada in ids_testada_list:
            layer_testada.SetAttributeFilter(f'id_testada = {id_testada}')
            sql = f'''
                SELECT a.id_alinhamento_predial, a.id_testada, b.StreetCode, a.geometry,
                    ST_Distance(a.geometry, b.geometry) AS dist
                FROM layer_testada a, layer_arruamento b
                WHERE a.id_testada = {id_testada}
                ORDER BY dist
                LIMIT 1
                '''
            query = self.datasource_entrada.ExecuteSQL(sql, dialect='SQLite')
            for row in query:
                layer_testada.StartTransaction()
                for feature in layer_testada:
                    if row.GetField('id_testada') == feature.GetField('id_testada'):
                        feature.SetField('StreetCode', row.GetField('StreetCode'))
                        layer_testada.SetFeature(feature)
                layer_testada.CommitTransaction()
            layer_testada.SetAttributeFilter(None)

        self.datasource_entrada.CopyLayer(layer_testada, 'layer_testada')

        self.datasource_entrada.ReleaseResultSet(query)

        return layer_testada
