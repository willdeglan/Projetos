from pathlib import Path

# Procura a pasta datasource a partir do diretório atual do notebook.
pastas = [Path.cwd(), *Path.cwd().parents]
candidatos = [p / "datasource" for p in pastas]
candidatos += [p / "i30" / "datasource" for p in pastas]

pasta_dados = next(
    (p for p in candidatos if (p / "registro_viagens.csv").is_file()),
    None
)

if pasta_dados is None:
    raise FileNotFoundError(
        f"Não encontrei datasource/registro_viagens.csv a partir de {Path.cwd()}"
    )

print(f"CSV encontrado em: {pasta_dados}")

