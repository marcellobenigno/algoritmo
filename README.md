# Algoritmo OO 

**Atenção!** tem um problema na função `cria_linhas_demandas` que é responsável por fazer o "zig-zag" nas demandas, encontrando as caixas secundárias.

Ela está repetindo o processo para algumas caixas (id_caixa). Isso fazer com o que o script praticamente não termine o seu processamento, quando uma área é maior. Para a `area_1` esse problema é mínimo, mas acontece:

```
cria linhas de demandas...
cria linha: 10.1.1
cria linha: 10.1.2
cria linha: 15.1.1
cria linha: 15.1.2
cria linha: 15.1.3
cria linha: 10.1.1
cria linha: 15.1.1
********************
ATENÇÃO! ESTA FUNÇÃO TEM UM PROBLEMA! ESTÁ REPETINDO O PROCESSO 
['10.1', '15.1', '9.1', '16.1', '10.1.1', '10.1.2', '15.1.1', '15.1.2', '15.1.3', '10.1.1', '15.1.1']
```

### O mesmo código está na brach `tratamento-de-erros`
