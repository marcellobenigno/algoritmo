from osgeo import ogr

ogr.UseExceptions()


class Arruamento:
    def __init__(self, datasource_entrada, layer='layer_arruamento'):
        self.datasource_entrada = datasource_entrada
        self.layer = layer

    def __str__(self):
        return self.get_layer().GetName()

    def get_layer(self):
        return self.datasource_entrada.GetLayer(self.layer)

    def get_srs(self):
        return self.get_layer().GetSpatialRef()

    def ordena_arruamento_por_comprimento(self, lista_street_code):
        street_codes = ','.join(str(street_code) for street_code in lista_street_code)

        sql = f'''
                SELECT 
                    "StreetCode",
                    geometry, 
                    ST_Length(geometry) AS comp 
                FROM {self.layer}
                WHERE "StreetCode" IN ({street_codes})
                ORDER BY comp DESC
              '''
        query = self.datasource_entrada.ExecuteSQL(sql, dialect='SQLite')

        arruamentos_ordenados = self.datasource_entrada.CopyLayer(
            query, 'arruamentos_ordenados'
        )

        self.datasource_entrada.ReleaseResultSet(query)

        return arruamentos_ordenados

    def cria_arruamento_recortado(self):
        lyr_arruamentos_recortados = self.datasource_entrada.CreateLayer(
            f"lyr_arruamento_recortado",
            geom_type=ogr.wkbLineString,
            srs=self.get_srs()
        )

        lyr_arruamentos_recortados.CreateField(
            ogr.FieldDefn("StreetCode", ogr.OFTInteger))
        lyr_arruamentos_recortados.CreateField(
            ogr.FieldDefn("id_caixa", ogr.OFTString))

        return lyr_arruamentos_recortados

    def recorta_arruamento(self, ponto_inicial, ponto_final, streetcode, id_caixa):
        sql = f'''
                SELECT StreetCode, {id_caixa},
                        ST_Line_Substring(
                            geometry,
                            ST_Line_Locate_Point(
                                geometry,
                                ST_ClosestPoint(
                                    geometry,
                                    ST_GeomFromText('{ponto_inicial}')
                                )
                            ),
                            ST_Line_Locate_Point(
                                geometry,
                                ST_ClosestPoint(
                                    geometry,
                                    ST_GeomFromText('{ponto_final}')
                                )
                            )
                        ) AS geometry
                FROM {self.layer}
                WHERE StreetCode = {streetcode}
        '''

        query = self.datasource_entrada.ExecuteSQL(sql, dialect="SQLite")

        lyr_arruamento_recortado = self.datasource_entrada.GetLayer('lyr_arruamento_recortado')

        lyr_arruamento_recortado.StartTransaction()

        for row in query:
            feature = ogr.Feature(lyr_arruamento_recortado.GetLayerDefn())
            feature.SetGeometry(row['geometry'])
            feature.SetField('StreetCode', streetcode)
            feature.SetField('id_caixa', id_caixa)
            lyr_arruamento_recortado.SetFeature(feature)

        lyr_arruamento_recortado.CommitTransaction()
        self.datasource_entrada.ReleaseResultSet(query)

        return lyr_arruamento_recortado

    def cria_arruamento_recortado_secundario(self, caixas_secundarias):

        lyr_arruamento_recortado = self.datasource_entrada.GetLayer('lyr_arruamento_recortado')
        lyr_arruamento_recortado_tmp = self.datasource_entrada.CopyLayer(
            lyr_arruamento_recortado,
            'lyr_arruamento_recortado_tmp'
        )
        lyr_linhas_demandas = self.datasource_entrada.GetLayer('layer_linhas_demandas')

        self.datasource_entrada.CopyLayer(
            lyr_arruamento_recortado,
            'lyr_arruamento_secundario'
        )

        for id_caixa in caixas_secundarias:
            if id_caixa.count('.') >= 2:
                id_atual = id_caixa[:-2]

                sql = f'''SELECT *
                        FROM layer_demandas_ordenadas
                        WHERE id_caixa = '{id_caixa}'
                        ORDER BY id ASC LIMIT 1                
                    '''
                query = self.datasource_entrada.ExecuteSQL(sql)

                feicao_inicial = query.GetNextFeature()
                ponto_inicial = feicao_inicial.GetGeometryRef().ExportToWkt()
                self.datasource_entrada.ReleaseResultSet(query)

                sql = f'''SELECT *
                    FROM layer_demandas_ordenadas
                    WHERE id_caixa = '{id_caixa}'
                    ORDER BY id DESC LIMIT 1                
                '''
                query = self.datasource_entrada.ExecuteSQL(sql)
                feicao_final = query.GetNextFeature()
                ponto_final = feicao_final.GetGeometryRef().ExportToWkt()
                self.datasource_entrada.ReleaseResultSet(query)

                sql = f'''
                        SELECT '{id_caixa}' AS id_caixa, StreetCode,
                                ST_Line_Substring(
                                    geometry,
                                    ST_Line_Locate_Point(
                                        geometry,
                                        ST_ClosestPoint(
                                            geometry,
                                            ST_GeomFromText('{ponto_inicial}')
                                        )
                                    ),
                                    ST_Line_Locate_Point(
                                        geometry,
                                        ST_ClosestPoint(
                                            geometry,
                                            ST_GeomFromText('{ponto_final}')
                                        )
                                    )
                                ) AS geometry
                        FROM lyr_arruamento_recortado_tmp
                        WHERE id_caixa = '{id_atual}'
                '''
                query = self.datasource_entrada.ExecuteSQL(sql, dialect="SQLite")

                lyr_arruamento_recortado.StartTransaction()

                for row in query:
                    if row['geometry']:
                        feature = ogr.Feature(lyr_arruamento_recortado.GetLayerDefn())
                        feature.SetGeometry(row['geometry'])
                        feature.SetField('StreetCode', row['streetcode'])
                        feature.SetField('id_caixa', id_caixa)
                        lyr_arruamento_recortado.SetFeature(feature)
                        break

                lyr_arruamento_recortado.CommitTransaction()
                self.datasource_entrada.ReleaseResultSet(query)

        self.datasource_entrada.DeleteLayer('lyr_arruamento_recortado_tmp')

        return lyr_arruamento_recortado
