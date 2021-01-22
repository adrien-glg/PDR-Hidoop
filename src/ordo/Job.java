package ordo;

import java.rmi.Naming;
import java.rmi.RemoteException;
import java.util.concurrent.Semaphore;
import config.Config;
import formats.Format.OpenMode;
import formats.Format.Type;
import hdfs.HdfsClient;
import formats.Format;
import formats.KVFormat;
import map.MapReduce;
import java.io.File;

public class Job implements JobInterface {

	private CallBack cb;
	protected Type inputFormat;
	protected String inputFile;
	protected int nbFrag;
	private Semaphore sem;

	public Job() {
		sem = new Semaphore(0);
		//On a un fragment par serveur (machine distante)
		this.nbFrag = Config.tab_serveurs.length;
	}

	@Override
	public void setInputFormat(Type ft) {
		this.inputFormat = ft;
	}

	@Override
	public void setInputFname(String fname) {
		this.inputFile = fname;

	}

	@Override
	public void startJob(MapReduce mr) {

		int nbMachines = nbFrag;
		int nbMachOccupees = 0;

		try {
			cb = new CallBackImpl(nbFrag, sem);
		} catch (Exception e) {
			e.printStackTrace();
		}

		try {
			while (!cb.isFinished()) {
				try {
					if (nbMachOccupees == nbMachines) {
						//On attend la terminaison d'un Map
						sem.acquire(); 
					} else {
						//On récupère l'adresse des fragments
						Format reader = new KVFormat(Config.PATH_SERVER[nbMachOccupees] + inputFile);

						//On devrait prendre en compte le format en entrée, mais par construction de 
						//hdfswrite on a forcément un KVFormat pour les fragments
						/*
						 * Format reader = null; if (inputFormat == Format.Type.LINE) { reader = new
						 * LineFormat(Config.PATH_SERVER[nbMachOccupees]+inputFile); } else { reader =
						 * new KVFormat(Config.PATH_SERVER[nbMachOccupees]+inputFile); }
						 */

						//On crée le fichier des résultats du map
						KVFormat writer = new KVFormat(Config.PATH_SERVER[nbMachOccupees] + "inter_" + inputFile);

						int port = 3000 + nbMachOccupees + 1;
						Worker worker = (Worker) Naming
								.lookup("//localhost:" + port + "/Worker" + (nbMachOccupees + 1));

						worker.runMap(mr, reader, writer, cb);
						nbMachOccupees++;
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		} catch (RemoteException e) {
			e.printStackTrace();
		}

		//REDUCE
		//On récupère tous les data tmp i, dans l'ordre.
		//HDFSClient va lancer HDFSRead qui va nous donner ce qu'on attend
		try {
			HdfsClient.HdfsRead("inter_" + inputFile);
		} catch (Exception e) {
			e.printStackTrace();
		}

		//On va lire le fichier des résultats produit précédemment
		Format tmp = new KVFormat(Config.PATH + "inter_" + inputFile);
		tmp.open(OpenMode.R); 

		//On va créer le fichier Res qu'on aura après le reduce de tmp
		Format Res = new KVFormat(Config.PATH + "resFinal_" + inputFile);
		Res.open(OpenMode.W);

		//On lance le reduce
		mr.reduce(tmp, Res);
		
		//Supprime les fichiers inter sur les serveurs
		HdfsClient.HdfsDelete("inter_" + inputFile);

		//Supprime les fragments générés par HdfsWrite
		HdfsClient.HdfsDelete(inputFile);

		//Supprimer le tmp
		new File(Config.PATH + "inter_"+inputFile).delete();

		
		
	}
	
	
}
