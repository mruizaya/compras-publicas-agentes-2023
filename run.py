import argparse
from agents import ecuador_agent, colombia_agent, chile_agent
from agents import utils

def main(year):
    print(f"Ejecutando pipeline para {year}...")
    # TODO: llamar agentes por país, luego normalizar y consolidar
    ecuador_agent.run(year)
    colombia_agent.run(year)
    chile_agent.run(year)
    utils.generate_report(year)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, default=2023)
    args = parser.parse_args()
    main(args.year)
