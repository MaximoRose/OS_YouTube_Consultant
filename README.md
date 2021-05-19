# Analyse statistiqus de chaîne YouTube

_N.B : Si vous réexecutez le fichier Jupyter NoteBook il lancera beaucoup d'exception. Je n'ai pas reporté l'ensemble des fichiers .json utilisés dans répertoire "dataset", mais j'en ai laissé un pour exemple de la structure utilisée_ 

Ce code permet d'analyser les statistiques de chaînes YouTube récoltées par l'intermédiaire d'un autre code pas encore publié (_give me time ! (and space, plase)_). Chaque chaîne requêtée produit un fichier .json que je dépose dans le répertoire "./dataset_with_tags/". Pour exemple, j'ai laissé le fichier "centrepompidou.json".

## Fichiers et répertoires.

- __dataset_mbr.py :__ classe d'objets qui permet d'initier les dataframes s'appuyant sur les fichiers .json. Ce module contient toutes les fonctions associées aux traitements de ces dataframe (si ce n'est l'exploitation des tags - pour l'instant).
- __public_YT2020_analysis.ipynb :__ est un exemple d'analyse que ce code permet de produire. C'est un fichier Jupyter NoteBook qui étudie ici la potentialité de promotion des contenus postés par les chaînes des insitutions culturelles requêtées.
