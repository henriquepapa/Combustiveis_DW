from Cargas import ibge, etanol_gasolina, glp, disel_gnv
from tqdm import tqdm

tqdm.write('Carga IBGE')
ibge.carga_ibge()

tqdm.write('Carga glp')
glp.carga_glp()

tqdm.write('Carga Gasolina e Etanol')
etanol_gasolina.carga_glp()

tqdm.write('Carga Disel e GNV')
disel_gnv.carga_glp()