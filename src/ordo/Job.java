package ordo;

import java.rmi.Naming;
import java.util.concurrent.Semaphore;

import config.Project;
import configuration.Setup;
import formats.Format.OpenMode;
import formats.Format.Type;
import hdfs.HdfsClient;
import formats.Format;
import formats.KVFormat;
import formats.LineFormat;
//import hdfs.NameNode; Implantï¿½?
import map.MapReduce;


public class Job implements JobInterface {
	
	private CallBack cb;
	protected Type inputFormat;
	protected String inputFile;
	protected int fragNb; //number of fragments = nb of Nodes
	private Semaphore sem;
	//private NameNode nameNode; Node?
	
	public Job( ) {
		sem = new Semaphore(0);
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
		
		int nbMachines = Project.listeMachines.length;
		int nbMachOccupees = 0;

		//NameNode NN = TODO Recupï¿½rer le NameNode
		//fragNb = NN.length ?
		//Rï¿½cuperer les pptï¿½s des machines
		
		try {
			cb = new CallBackImpl(fragNb, sem);
		} catch(Exception e) {
			e.printStackTrace();
		}
	
		while (!cb.isFinished()) {
			try {
				if (nbMachOccupees == nbMachines) {
					sem.acquire(); // On attend la terminaison d'un Map
				} else {
					//TODO ON PREND L'ID D'UNE MACHINE
					String idMachine = Project.listeMachines[nbMachOccupees];
					
					//TODO ON RECUP L'ADRESSE DE DATA PART
					//emplacements temporaires
					//LineFormat reader = new LineFormat(Setup.PATH + Setup.DATAN7 + inFname + "@"+ idMachine);
					LineFormat reader = new LineFormat("/home/dtrinh/Bureau/Hidoop" + "/data" + inputFile + "@"+ nbMachOccupees);
					
					//TODO ON RECUP L'ADRESSE DE DATA TMP
					//KVFormat writer = new KVWriter(Setup.PATH + Setup.DATAN7 + inFname + "res@"+ idMachine)
					KVFormat writer = new KVFormat("/home/dtrinh/Bureau/Hidoop" + "/data" + inputFile + "res@"+ nbMachOccupees);
					
					String idMach = Project.listeMachines[nbMachOccupees];
					//int port = Project.listePorts[nbMachOccupees];
					//Worker worker = (Worker) Naming.lookup("//" + idMach + ":" + port + "/Worker");
					Worker worker = (Worker) Naming.lookup("//localhost/Worker"+nbMachOccupees);
					worker.runMap(mr, reader, writer, cb);
					nbMachOccupees++;
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			
		
		//TODO ON REDUCE
		//On récupère tous les data tmp, dans l'ordre.
		//HDFSClient va lancer HDFSRead qui va nous donner ce qu'on attend
		//On pourra le récuperer à l'emplacement 
		// "/home/dtrinh/Bureau/Hidoop" + "/data" + inputFile + "resLu"
		String[] args = {"read", inputFile + "res", inputFile };
		HdfsClient.main(args);
		
		//On va lire le fichier des résultats produit précédemment
		//Format tmp = new KVFormat(Setup.PATH + Setup.DATAN7 + inputFile + "resLu");
		Format tmp = new KVFormat("/home/dtrinh/Bureau/Hidoop" + "/data" + inputFile + "resLu");
		tmp.open(OpenMode.R); 
		
		//On va créer le fichier Res qu'on aura après le reduce de tmp
		//Format Res = new KVFormat(Setup.PATH + Setup.DATAN7 + inputFile + "@res");
		Format Res = new KVFormat("/home/dtrinh/Bureau/Hidoop" + "/data" + inputFile + "@res");
		Res.open(OpenMode.W);
		
		//On lance le reduce
		mr.reduce(tmp, Res);
		
		//TODO SUPPRIMER LES FICHERS INUTILES
		
		}
		
	}
	
	
}
