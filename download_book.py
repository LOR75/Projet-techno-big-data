import os
import sparknlp
from git import Repo

def sparse_clone(remote_url, folder_to_keep, local_path):
    if not os.path.exists(local_path):
        os.makedirs(local_path)
    
    repo = Repo.init(local_path)
    
    if 'origin' in [remote.name for remote in repo.remotes]:
        origin = repo.remote('origin')
    else:
        origin = repo.create_remote('origin', remote_url)
    
    repo.git.sparse_checkout('init', '--cone')
    repo.git.sparse_checkout('set', folder_to_keep)
    
    origin.pull('master') 
    
    print(f"Le dossier '{folder_to_keep}' et tout son contenu sont là. Propre et efficace, comme le GOAT LeBron ! 🏀🏀")

sparse_clone("https://github.com/dh-trier/balzac.git", "./plain", "./ressource")