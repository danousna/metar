# NF26 - METAR

Les données à analyser sont théoriquement celle de l'Allemagne sur la période de 2001 à 2010. Néanmoins, aucune données n'est pas disponible pour cette période. Il faut donc choisir un autre pays.

## Pour développer 

Pour uploader automatiquement les fichiers vers le serveur :

- Installer `fswtach` et `rsync` si nécessaire
- fswatch -0 -o -e .git/ . | xargs -0 -I {} rsync -av --update --exclude=.git . danousna@nf26-1.leger.tf:metar/

## Cassandra

- `ssh [login]@nf26-3.leger.tf`
- `cqlsh`
- `use [keyspace] ;`
