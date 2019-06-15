# NF26 - METAR

Les données à analyser sont théoriquement celle de l'Allemagne sur la période de 2001 à 2010. Néanmoins, aucune données n'est pas disponible pour cette période. Il faut donc choisir un autre pays.

## Installation

- `virtualenv venv`
- `source venv/bin/activate`
- `pip install -r requirements.txt`

## Cassandra

- `ssh [login]@nf26-3.leger.tf`
- `cqlsh`
- `use use danousna_metar ;`
