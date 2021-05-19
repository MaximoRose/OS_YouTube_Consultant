# Analyse statistique de chaînes YouTube

_N.B : Si vous réexecutez le fichier Jupyter NoteBook il lancera beaucoup d'exceptions. Je n'ai pas reporté l'ensemble des fichiers .json utilisés dans le répertoire "dataset_with_tags", mais j'en ai laissé un pour exemple de la structure utilisée_ 

Ce code permet d'analyser les statistiques de chaînes YouTube récoltées par l'intermédiaire d'un autre code pas encore publié (_give me time_). Chaque chaîne requêtée produit un fichier .json que je dépose dans le répertoire "./dataset_with_tags/". Pour exemple, j'ai laissé le fichier "centrepompidou.json".

## Fichiers et répertoires.

- __dataset_mbr.py :__ classe d'objets qui permet d'initier les dataframes s'appuyant sur les fichiers .json. Ce module contient toutes les fonctions associées aux traitements de ces dataframe (si ce n'est l'exploitation des tags - pour l'instant) : moyennes lissées de vues, de posts, etc.
- __public_YT2020_analysis.ipynb :__ est un exemple d'analyse que permet ce code. C'est un fichier Jupyter NoteBook qui étudie ici la potentialité de promotion des contenus postés par les chaînes des insitutions culturelles requêtées.
- __/dataset_with_tags/ :__ est le dossier où je dépose les fichiers .json des chaînes YouTube que je requête. Un code que je n'ai pas encore publié, me permet, à partir d'une liste airtable, d'aller chercher les informations des vidéos postées par les chaînes référencées en exploitant les API YouTube. Ce sont ces fichiers .json qui permettent de créer les dataframes qui j'exploite dans le NoteBook. Le dataset utilisé pour le fichier NoteBook d'exemples contenait donc 136 fichiers .json.
