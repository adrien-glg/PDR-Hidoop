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
import formats.LineFormat;
import map.MapReduce;
import java.io.File;

public class Job implements JobInterface {

	private CallBack cb;
	protected Type inputFormat;
	protected String inputFile;
	protected int nbFrag; // number of fragments = nb of Nodes
	private Semaphore sem;

	public Job() {
		sem = new Semaphore(0);
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
				System.out.println("hello " + nbMachOccupees);
				try {
					if (nbMachOccupees == nbMachines) {
						sem.acquire(); // On attend la terminaison d'un Map
					} else {
						// TODO ON PREND L'ID D'UNE MACHINE
						// String idMachine = Project.listeMachines[nbMachOccupees];

						// TODO ON RECUP L'ADRESSE DE DATA PART
						// emplacements temporaires
						// LineFormat reader = new LineFormat(Project.PATH + Project.DATAN7 + inFname +
						// "@"+ idMachine);
						Format reader = new KVFormat(Config.PATH_SERVER[nbMachOccupees] + inputFile);

						/*
						 * Format reader = null; if (inputFormat == Format.Type.LINE) { reader = new
						 * LineFormat(Config.PATH_SERVER[nbMachOccupees]+inputFile); } else { reader =
						 * new KVFormat(Config.PATH_SERVER[nbMachOccupees]+inputFile); }
						 */

						// TODO ON RECUP L'ADRESSE DE DATA TMP
						// KVFormat writer = new KVWriter(Project.PATH + Project.DATAN7 + inFname +
						// "res@"+ idMachine)
						KVFormat writer = new KVFormat(Config.PATH_SERVER[nbMachOccupees] + "inter_" + inputFile);

						// int port = Project.listePorts[nbMachOccupees];
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
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		//TODO ON REDUCE
		//On récupère tous les data tmp i, dans l'ordre.
		//HDFSClient va lancer HDFSRead qui va nous donner ce qu'on attend
		String[] args = {"read", "inter_" + inputFile};
		HdfsClient.main(args);

		//On va lire le fichier des résultats produit précédemment
		//Format tmp = new KVFormat(Setup.PATH + Setup.DATAN7 + inputFile + "resLu");
		Format tmp = new KVFormat(Config.PATH + "inter_" + inputFile);
		tmp.open(OpenMode.R); 

		//On va créer le fichier Res qu'on aura après le reduce de tmp
		//Format Res = new KVFormat(Setup.PATH + Setup.DATAN7 + inputFile + "@res");
		Format Res = new KVFormat(Config.PATH + "resFinal_" + inputFile);
		Res.open(OpenMode.W);

		//On lance le reduce
		mr.reduce(tmp, Res);
		/*
		//TODO SUPPRIMER LES FICHERS INUTILES
		//supprime les fichiers inter sur les serveur
		String[] args2 = {"delete", "inter_" + inputFile};
		HdfsClient.main(args2);

		//Supprimer le tmp
		new File(Config.PATH + "inter_"+inputFile).delete();

		*/
		
	}
	
	
}
