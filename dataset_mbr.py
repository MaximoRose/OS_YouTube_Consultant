# Classe de traitement des fichiers json pour en faire des Listes de chaines YT ou des listes de videos YT

import json
# importer les librairies pour travailler sur les dossiers et fichiers // source : https://stackoverflow.com/questions/3207219/how-do-i-list-all-files-of-a-directory
from os import listdir
from os.path import isfile, join
# importer datetime pour typer les donnees de dates des chaines et des videos
import datetime
import pandas as pd
import math
# necessaire pour les projection 
import random
# necessaire pour la conversion de la duree de la video Youtube
import isodate

# Transforme une colonne de listes en colonne d'une seule dimension avec toutes les valeurs
# Permet de faire des stats en volume d'occurences d'un tag de vidéo ou de chaîne rapidement.
# Thanks : https://towardsdatascience.com/dealing-with-list-values-in-pandas-dataframes-a177e534f173
def listcol_to_1dcol(series) :
    return pd.Series(x for _list in series for x in _list)


# -----------------------------------------------------------------------------------------------------------------
# Objet DataSets associé a ma fonction de requetage Youtube qui définit le format des fichiers .json qu'on exploite
# -----------------------------------------------------------------------------------------------------------------
class mb_ds :

    def __init__(self) :
        self.dataset = None
        self.datachaines = None
        self.datavideos = None

    # --------------------------------------------------------------------------------------
    # Load Files in memory / crée un dataframe contenant les fichiers .json mis bout-à-bout
    # --------------------------------------------------------------------------------------
    def get_files_dt(self, folder) :
        myfolder = folder
        #Recupere la liste des fichiers dans le dossier folder 
        onlyfiles = [f for f in listdir(myfolder) if isfile(join(myfolder, f))]

        data_set = []
        data = None
        for filen in onlyfiles :
            with open(myfolder+filen, 'r') as f:
                # print(f)
                data = json.load(f)
                data_set.append(data)

        del data

        print('Imported '+str(len(data_set))+' channel data')

        self.dataset = data_set

        return

    # --------------------------------------------------------------------------------------------------------------------
    # Load full data in two dataFrames : One for stats by channel / One for stats by videos
    # Crée deux dataframes au format plus exploitables : Un contient les stats des chaînes, l'autre de toutes les vidéos
    # --------------------------------------------------------------------------------------------------------------------
    def get_full_data(self):
        # Puisque veut faire des dataframes, il va falloir que je reagrege les donnees des videos sous la forme d'une seule ligne comme dans un tableau Excel
        dataset_chaines = []
        dataset_videos = []
        cnt = 0

        if self.dataset is not None :
            for datachaine in self.dataset :
                yt_associatedVideosData = None
                yt_channelName = ''
                cnt += 1
                # J'imagine que la fonction pop est mieux pour la portabilite du code, mais ballek
                try :
                    dateRqt = datachaine['dateRequete']
                except KeyError :
                    dateRqt = '1970/01/01'
                try :
                    secteurOrga = datachaine['SecteurOrga']
                except KeyError :
                    secteurOrga = 'Undefined'
                try : 
                    secteurType = datachaine['SecteurType']
                except KeyError :
                    secteurType = 'Undefined'
                try :
                    channeldata = datachaine['YoutubeChannel'].popitem()
                except KeyError :
                    channeldata = 'corrupted data'

                if channeldata != 'corrupted data':
                    # L'element 0 de cette liste est l'id de la chaine
                    try :
                        yt_channelName = channeldata[1]['channel_info']['title']
                    except KeyError :
                        yt_channelName = 'Unknown'
                    try :
                        chaine_publishedAt = channeldata[1]['channel_info']['publishedAt']
                        chaine_publishedAt = chaine_publishedAt[0:chaine_publishedAt.find('T')]
                        yt_dateCreationChaine = datetime.date(int(chaine_publishedAt[0:4]), int(chaine_publishedAt[5:7]), int(chaine_publishedAt[8:10]))
                    except KeyError :
                        yt_dateCreationChaine = datetime.datetime(1970, 1, 1)
                    try :
                        yt_country = channeldata[1]['channel_info']['country']
                    except KeyError :
                        # En cas de doute, on set la chaine en FR - à modifier en Unknown à l'avenir
                        yt_country = 'FR'
                    try :
                        yt_globalChannelStats = channeldata[1]['channel_info']['statistics']
                    except KeyError :
                        yt_globalChannelStats = None
                    try :
                        yt_channel_categories = channeldata[1]['channel_info']['channelcategories']
                    except KeyError :
                        yt_channel_categories = None
                    try :
                        yt_associatedVideosData = channeldata[1]['video_data']
                    except KeyError :
                        yt_associatedVideosData is None

                    if yt_globalChannelStats is not None :
                            try :
                                yt_vuesChaines = int(yt_globalChannelStats['viewCount'])
                            except KeyError :
                                yt_vuesChaines = 0
                            try :
                                yt_abonnes = int(yt_globalChannelStats['subscriberCount'])
                            except KeyError :
                                yt_abonnes = 0
                            try :
                                yt_vidcount = int(yt_globalChannelStats['videoCount'])
                            except KeyError :
                                yt_vidcount = 0
                            # 21-3-13 : Ajout de country apres date creation chaine / Avant vue chaine
                            dataset_chaines.append([dateRqt, secteurOrga, secteurType, yt_channelName, yt_dateCreationChaine, yt_country, yt_channel_categories, yt_vuesChaines, yt_abonnes, yt_vidcount])

                    # Si la chaîne dispose bien de vidéos, on va récupérer leurs stats pour les intégrer au dataframe attendu dédié
                    if yt_associatedVideosData is not None :
                        try :
                            sorted_vids = sorted(yt_associatedVideosData.items(), key = lambda item: int(item[1]['viewCount']), reverse = True)
                        except KeyError :
                            print('Probleme avec chaine : ' + yt_channelName)

                        # On recupere les informations de notre liste triee
                        for vid in sorted_vids:
                            try :
                                vid_title = vid[1]['title']
                            except KeyError :
                                vid_title = 'Unknown'
                            try :
                                video_publishedAt = vid[1]['publishedAt']
                                # 21-3-12 :
                                vid_datepub = datetime.datetime(int(video_publishedAt[0:4]), int(video_publishedAt[5:7]), int(video_publishedAt[8:10]), int(video_publishedAt[11:13]), int(video_publishedAt[14:16]), int(video_publishedAt[14:16]))

                            except KeyError :
                                vid_datepub = datetime.datetime(1970, 1, 1)
                            # 21-5-3
                            try :
                                video_tag_list = vid[1]['tags']
                            except KeyError :
                                video_tag_list = None
                            try :
                                video_livebroadcast = vid[1]['livebroadcast']
                            except :
                                video_livebroadcast = "none"
                            try : 
                                vid_views = int(vid[1]['viewCount'])
                            except KeyError :
                                vid_views = 0
                            try : 
                                vid_likes = int(vid[1]['likeCount'])
                            except KeyError :
                                vid_likes = 0
                            try :
                                vid_dislikes = int(vid[1]['dislikeCount'])
                            except KeyError :
                                vid_dislikes = 0
                            try :
                                vid_favcount = int(vid[1]['favoriteCount'])
                            except KeyError :
                                vid_favcount = 0
                            try : 
                                vid_comments = int(vid[1]['commentCount'])
                            except KeyError :
                                vid_comments = 0
                            try :
                                vid_duration = isodate.parse_duration(vid[1]['duration'])
                            except KeyError :
                                vid_duration = pd.Timedelta('0 days 00:00:00')
                            dataset_videos.append([dateRqt, secteurOrga, secteurType, yt_channelName, vid_title, vid_datepub, video_tag_list, video_livebroadcast, vid_views, vid_likes, vid_dislikes, vid_favcount, vid_comments, vid_duration])

                else :
                    print('Corrupted data on '+str(cnt)+'th element. channel >> '+str(datachaine))
                    continue     
        else :
            print('Instancier le dataset dans le bon dossier avant de generer les deux dataFrames')

        df_chaine = pd.DataFrame(dataset_chaines, columns=['dateRqt', 'secteur', 'structure', 'chaine', 'dateCreationChaine', 'pays', 'categorieschaine','vuesChaine','abonnesChaine','nbVideosChaine'])
        df_video = pd.DataFrame(dataset_videos, columns=['dateRqt', 'secteur', 'structure', 'chaine','titreVideo','dateVideo', 'liste_tags', 'livebroadcast', 'vuesVideo', 'likesVideo','dislikesVideo', 'favVideo', 'comzVideo', 'dureeVideo'])

        self.datavideos = df_video
        self.datachaines = df_chaine

        return df_video, df_chaine

    # --------------------------------------------------------------------------------------
    # Retourne un dataframe de chaînes disposant de la moyenne des vues par vidéos 
    # --------------------------------------------------------------------------------------
    def get_df_chaine_mv(self, datemin = datetime.datetime(1970,1,1, 0, 0, 0), datemax = datetime.datetime(2100, 1, 1, 23, 59, 59), top=0) :
        # Puisque veut faire des data frame, il va falloir que je reagrege les donnees des videos sous la forme d'une seule ligne comme dans un tableau Excel
        dataset_chaines_mv = []

        for ind in self.datachaines.index :
            # print(ligne)
            # l'element 3 de ligne est la chaine (rappel : on demarre a 0)
            mv_chaine = self.get_mean_views_channel(self.datachaines['chaine'][ind], datemin=datemin, datemax=datemax, top=top)
            # print('Moyenne des vues de la chaine ' + str(ligne)+ ' : '+ str(int(mv_chaine)))
            dataset_chaines_mv.append([self.datachaines['chaine'][ind], self.datachaines['secteur'][ind], self.datachaines['structure'][ind], self.datachaines['vuesChaine'][ind], self.datachaines['abonnesChaine'][ind],  self.datachaines['nbVideosChaine'][ind], mv_chaine])

        df_chaine = pd.DataFrame(dataset_chaines_mv, columns=['chaine', 'secteur', 'structure', 'vuesChaine', 'abonnesChaine', 'nbVideosChaine', 'video_mv'])

        return df_chaine



    # ------------------------------------------------------------------------------------------------------------------------------------------------------------------
    # Retourne la moyenne de vues des videos d'une chaine passée en paramètre. On peut aussi définir des critères de dates de début et de date de fin
    # ------------------------------------------------------------------------------------------------------------------------------------------------------------------
    # get mean views of channel
    # Possibilite de borner sur une periode en date
    # Possibilite de ne prendre que le top
    def get_mean_views_channel (self, channel, datemin = datetime.datetime(1970,1,1,0,0,0), datemax = datetime.datetime(2100, 1, 1, 23, 59, 59), top=0) :
        df_chan = self.datavideos[self.datavideos['chaine'] == channel]
        if (datemin == datetime.datetime(1970,1,1, 0, 0, 0) and datemax == datetime.datetime(2100, 1, 1, 23, 59, 59)) :
            if top != 0 :
                df_chan = df_chan.sort_values(by = ['vuesVideo'], ascending = False).head(top)
            meanV = df_chan['vuesVideo'].mean()
        elif datemax == datetime.datetime(2100, 1, 1, 23, 59, 59) :
            df_chan_datemin = df_chan[df_chan['dateVideo'] > datemin]
            if top != 0 :
                df_chan = df_chan.sort_values(by = ['vuesVideo'], ascending = False).head(top)
            meanV = df_chan_datemin['vuesVideo'].mean()
        else : 
            df_chan_datemax = df_chan[df_chan['dateVideo'] < datemax]
            if top != 0 :
                df_chan = df_chan.sort_values(by = ['vuesVideo'], ascending = False).head(top)
            meanV = df_chan_datemax['vuesVideo'].mean()
        return meanV


    # -------------------------------------------------------------------------------------------------------------------
    # Retourne la moyenne de vues des videos d'une chaine, d'un secteur ou d'une structure sur le mois d'une date donnée
    # -------------------------------------------------------------------------------------------------------------------
    # fonction qui retourne la mean value d'un secteur ou d'une chaine sur un mois
    def vid_month_meanview(self, secteur='', channel='', structure='', isnot=False, date=datetime.datetime.now, wo_struct = '', top=0) :
        meanV = 0
        df = self.datavideos
        if isnot is False :
            if secteur !='' :
                df = df[df['secteur'] == secteur]
                if wo_struct != '' :
                    df = df[df['structure'] != wo_struct]
            elif channel != '' :
                df = df[df['chaine'] == channel]
            elif structure != '' :
                df = df[df['structure'] == structure]
        else :
            if secteur !='' :
                df = df[df['secteur'] != secteur]
            elif channel != '' :
                df = df[df['chaine'] != channel]
            elif structure != '' :
                df = df[df['structure'] != structure]
        datemin = datetime.datetime(date.year, date.month, 1, 0, 0, 0)

        # Au dela de 12 mois, on augmente l'annee
        if date.month < 12 :
            datemax = datetime.datetime(date.year, date.month + 1, 1, 0, 0, 0)
        else :
            datemax = datetime.datetime(date.year + 1, date.month, 1, 0, 0, 0)

        df = df[df['dateVideo'] >= datemin]
        df = df[df['dateVideo'] < datemax]
        if top == 0 :
            meanV = df['vuesVideo'].mean()
        else :
            df = df.sort_values(by=['vuesVideo'], ascending=False).head(top)
            meanV = df['vuesVideo'].mean()
        return meanV
    
    # ----------------------------------------------------------------------------------------------------
    # Nombre de video sur une periode donnee en fonction des parametres
    # ----------------------------------------------------------------------------------------------------
    # Retourne le nombre de videos sur une periode donnee
    def nbvid_period (self, secteur='', chaine='', datemin=datetime.datetime(1970, 1, 1, 0, 0, 0), datemax=datetime.datetime(2100, 1, 1, 0, 0, 0)) :
        dv = self.datavideos
        if secteur != '' :
            dv = dv[dv['secteur'] == secteur]
        if chaine != '' :
            dv = dv[dv['chaine'] == chaine]
        if datemin != datetime.datetime(1970, 1, 1, 0, 0, 0) :
            dv = dv[dv['dateVideo'] > datemin]
        dv = dv[dv['dateVideo'] < datemax]
        return dv.shape[0]

    # ----------------------------------------------------------------------------------------------------
    # Nombre de videos postees par une chaine sur un mois
    # ----------------------------------------------------------------------------------------------------
    def nbvid_chainemois (self, channel='', date=datetime.datetime.now) :
        df = self.datavideos[self.datavideos['chaine'] == channel]
        datemin = datetime.datetime(date.year, date.month, 1, 0, 0, 0)

        # Au dela de 12 mois, on augmente l'annee
        if date.month < 12 : 
            datemax = datetime.datetime(date.year, date.month + 1, 1, 0, 0, 0)
        else :
            datemax = datetime.datetime(date.year + 1, date.month, 1, 0, 0, 0)

        df = df[df['dateVideo'] >= datemin]
        df = df[df['dateVideo'] < datemax]

        return df.shape[0]

    # ----------------------------------------------------------------------------------------------------
    # Liste des videos postees par un secteur sur un mois
    # Retourne un dataframe { chaine ; nombre de vidéos postées sur le mois }
    # ----------------------------------------------------------------------------------------------------
    def vid_secteur_mois (self, secteur='', date=datetime.datetime.now,  wo_struct = '', structure = '') :
        dc = self.datachaines
        if secteur != '' :
            dc = dc[dc['secteur'] == secteur] 
        if structure != '' :
            dc = dc[dc['structure'] == structure]
        if wo_struct != '' :
            dc = dc[dc['structure'] != wo_struct]

        nbv_par_chaine = []

        for chaine in dc['chaine'] :
            nbv = self.nbvid_chainemois(chaine, date)
            nbv_par_chaine.append([chaine, nbv])

        df_nbvchaine = pd.DataFrame(nbv_par_chaine, columns=['chaine', 'nbVideos'])
        return df_nbvchaine

    # --------------------------------------------------------------------------------------
    # Retourne un dataframe de vidéos avec, par ligne, le taux de like de la video = nbVues / nblikes
    # --------------------------------------------------------------------------------------
    def get_df_video_tauxlike(self) :
        dsv_tauxlikes = []

        for ind in self.datavideos.index :
            tl = 0.0
            tdl = 0.0
            total_act = 0.0
            vidname = self.datavideos['titreVideo'][ind]
            vidview = self.datavideos['vuesVideo'][ind]
            vidlikes = self.datavideos['likesVideo'][ind]
            viddisl = self.datavideos['dislikesVideo'][ind]

            # je fais des conversion de batard parce que j'ai un probleme de type
            try :
                s_vv = str(vidview)
                s_vl = str(vidlikes)
                s_vd = str(viddisl)
                i_vv = int(s_vv)
                i_vl = int(s_vl)
                i_vd = int(s_vd)
            except :
                print('error with : '+ vidname + ', vidlikes : '+ str(vidlikes) + ', viddisl : '+ str(viddisl))


            if i_vv != 0 :
                tl = i_vl/i_vv

            try :
                total_act = i_vl + i_vd
            except :
                print('error with : '+ vidname + ', vidlikes : '+ str(vidlikes) + ', viddisl : '+ str(viddisl))

            if total_act !=0 :
                tdl = i_vd/total_act

            dsv_tauxlikes.append([self.datavideos['chaine'][ind], self.datavideos['secteur'][ind], self.datavideos['structure'][ind], vidname, self.datavideos['dateVideo'][ind], vidview, vidlikes, viddisl, tl, tdl])

        df_vid = pd.DataFrame(dsv_tauxlikes, columns=['chaine', 'secteur', 'structure', 'titreVideo', 'dateVideo', 'vuesVideo', 'likesVideo', 'dislikesVideo', 'tauxlikes', 'tauxDislikes'])

        return df_vid

    # ------------------------------------------------------------------------------------------------------------------------------------
    # Deduit une part supposée de trafic permis par la plateforme pour evaluer la part de vues apportées par les chaînes qui postent
    # N.B : Cette fonction n'est pas utilisée dans le NoteBook public
    # -------------------------------------------------------------------------------------------------------------------------------------
    # coef en parametre 
    def proj_delta_yt_algo(self, secteur = '', isnot = False, datemin = datetime.datetime(1970, 1, 1, 0, 0, 0), datemax = datetime.datetime(2100, 1, 1, 0, 0, 0), coefminvue = 0.25, minvue = 5000, coefvue = 0.5, moyvue = 50000, coefmaxvue = 0.6) :
        # Initialisation dataframe
        list_proj = []
        sumv = 0
        og_trafic = 0 #original gangster
        dv = self.datavideos
        if not isnot :
            if secteur != '' :
                dv = dv[dv['secteur'] == secteur]
        else :
            dv = dv[dv['secteur'] != secteur]
        dctp = dv[dv['dateVideo'] > datemin]
        dctp = dctp[dctp['dateVideo'] < datemax]


        for i, row in dctp.iterrows():
            if row['vuesVideo'] < minvue :
                 og_trafic = (1 - coefminvue)*row['vuesVideo']
            elif row['vuesVideo'] < moyvue :
                 og_trafic = (1 - coefvue)*row['vuesVideo']
            else :
                 og_trafic = (1 - coefmaxvue)*row['vuesVideo']
            list_proj.append(og_trafic)

        
        sumv = sum(list_proj)

        return sumv

    # ---------------------------------------------------------------------------------------------------
    # Simule un effet rebond en imaginant qu'une vue a 'x'% de chance de provoquer une vue additionnelle
    # N.B : Cette fonction n'est pas utilisée dans le NoteBook public
    # ---------------------------------------------------------------------------------------------------
    # Je pense que ça pourrait être grandement optimisé 
    def proj_effet_tunnel(self, nbtirages=10, nbchance=1, nbvue=0) :
        if nbvue == 0 :
            print('Please, set view')
            return 0

        newnbvue = nbvue

        if nbtirages > 0 :
            for i in range(int(nbvue)) :
                chance = random.randint(1,nbtirages)
                if chance in range(1,nbchance+1) :
                    newnbvue+=1
    
        else :
            print("don't be ridiculous")
        
        return newnbvue


    # Return oldest video of channel
    def oldest_vide(self, limit = 50, secteur = '', chaine = '') :
        # Creation des dataFrame pour nombre de chaines
        dc = self.datachaines
        if secteur != '' :
            dc = dc[dc['secteur'] == secteur]
        if chaine != '' :
            dc = dc[dc['chaine'] == chaine]
        dc = dc.sort_values(by=['nbVideosChaine'], ascending=False)

        listrec = []
        for ind in dc.index :
            if (dc['nbVideosChaine'][ind] > limit) :
                vc = self.datavideos[self.datavideos['chaine'] == dc['chaine'][ind]]
                nbvids = vc.shape[0]
                vc = vc.sort_values(by=['dateVideo'], ascending=True)
                oldestRec = vc.head(1)
                listrec.append(oldestRec)

        return listrec


    # ---------------------------------------------------------------------------------------------------
    # Retourne les tops vues pour une durée précisée
    # Permet de déterminer des niches par secteur
    # N.B : Cette fonction n'est pas utilisée dans le NoteBook public
    # ---------------------------------------------------------------------------------------------------
    # coef en parametre 
    def vid_duree_meanview(self, secteur='', channel='', structure='', dureemin=pd.Timedelta('0 days 00:00:00'), dureemax=pd.to_timedelta('3 days 00:00:00'), wo_struct = '', top=0, isnot=False) :
        meanV = 0
        df = self.datavideos
        if isnot is False :
            if secteur !='' :
                df = df[df['secteur'] == secteur]
                if wo_struct != '' :
                    df = df[df['structure'] != wo_struct]
            elif channel != '' :
                df = df[df['chaine'] == channel]
            elif structure != '' :
                df = df[df['structure'] == structure]
        else :
            if secteur !='' :
                df = df[df['secteur'] != secteur]
            elif channel != '' :
                df = df[df['chaine'] != channel]
            elif structure != '' :
                df = df[df['structure'] != structure]
        
        # Enelever des data les videos pour lesquelles on n'a pas eu de duration :
        df = df[df['dureeVideo'] != pd.to_timedelta('0 days 00:00:00')]
        # print('avant transfo : '+ str(df.shape[0]))
        df = df[df['dureeVideo'] >= dureemin]
        # print('apres datemin : '+ str(df.shape[0]))
        df = df[df['dureeVideo'] < dureemax]

        # print('apres datemax : '+ str(df.shape[0]))

        df = df.sort_values(by=['vuesVideo'], ascending=False)
        if top == 0 :
            meanV = df['vuesVideo'].mean()
        else :
            df = df.head(top)
            meanV = df['vuesVideo'].mean()
        return meanV

    # ---------------------------------------------------------------------------------------------------
    # Retourne les tops vues pour une duree
    # Je ne sais plus à quoi sert cette fonction, elle ressemble drôlement à celle du dessus.
    # ---------------------------------------------------------------------------------------------------
    # coef en parametre 
    def df_vid_duree(self, secteur='', channel='', structure='', dureemin=pd.Timedelta('0 days 00:00:00'), dureemax=pd.to_timedelta('3 days 00:00:00'), wo_struct = '', isnot=False) :
        meanV = 0
        df = self.datavideos
        if isnot is False :
            if secteur !='' :
                df = df[df['secteur'] == secteur]
                if wo_struct != '' :
                    df = df[df['structure'] != wo_struct]
            elif channel != '' :
                df = df[df['chaine'] == channel]
            elif structure != '' :
                df = df[df['structure'] == structure]
        else :
            if secteur !='' :
                df = df[df['secteur'] != secteur]
            elif channel != '' :
                df = df[df['chaine'] != channel]
            elif structure != '' :
                df = df[df['structure'] != structure]
        
        # Enelever des data les videos pour lesquelles on n'a pas eu de duration :
        df = df[df['dureeVideo'] != pd.to_timedelta('0 days 00:00:00')]
        # print('avant transfo : '+ str(df.shape[0]))
        df = df[df['dureeVideo'] >= dureemin]
        # print('apres datemin : '+ str(df.shape[0]))
        df = df[df['dureeVideo'] < dureemax]

        # print('apres datemax : '+ str(df.shape[0]))

        df = df.sort_values(by=['vuesVideo'], ascending=False)
        
        return df

    
