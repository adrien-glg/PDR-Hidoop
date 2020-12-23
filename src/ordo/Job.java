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
//import hdfs.NameNode; Implant�?
import map.MapReduce;


public class Job implements JobInterface {
	
	private CallBack cb;
	protected Type inputFormat;
	protected String inputFile;
	protected int nbMachine; //number of fragments �galement
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
		
		this.nbMachine = Project.listeMachines.length;
		int nbMachOccupees = 0;

		
		try {
			cb = new CallBackImpl(this.nbMachine, sem);
		} catch(Exception e) {
			e.printStackTrace();
		}
	
		while (!cb.isFinished()) {
			try {
				if (nbMachOccupees == this.nbMachine) {
					sem.acquire(); // On attend la terminaison d'un Map
				} else {
					//TODO ON PREND L'ID D'UNE MACHINE
					String idMachine = Project.listeMachines[nbMachOccupees];
					
					//TODO ON RECUP L'ADRESSE DE DATA PART
					//emplacements temporaires
					//LineFormat reader = new LineFormat(Setup.PATH + Setup.DATAN7 + inFname + "@"+ idMachine);
					Format reader = null;
					if (inputFormat == Format.Type.LINE) {
						reader = new LineFormat("/home/dtrinh/Bureau/Hidoop" + "/data" + inputFile + "@"+ nbMachOccupees);
					} else {
						reader = new KVFormat("/home/dtrinh/Bureau/Hidoop" + "/data" + inputFile + "@"+ nbMachOccupees);
					}
					
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
		//On r�cup�re tous les data tmp i, dans l'ordre.
		//HDFSClient va lancer HDFSRead qui va nous donner ce qu'on attend
		//On pourra le r�cuperer � l'emplacement 
		// "/home/dtrinh/Bureau/Hidoop" + "/data" + inputFile + "resLu"
		String[] args = {"read", inputFile + "res", inputFile };
		HdfsClient.main(args);
		
		//On va lire le fichier des r�sultats produit pr�c�demment
		//Format tmp = new KVFormat(Setup.PATH + Setup.DATAN7 + inputFile + "resLu");
		Format tmp = new KVFormat("/home/dtrinh/Bureau/Hidoop" + "/data" + inputFile + "resLu");
		tmp.open(OpenMode.R); 
		
		//On va cr�er le fichier Res qu'on aura apr�s le reduce de tmp
		//Format Res = new KVFormat(Setup.PATH + Setup.DATAN7 + inputFile + "@res");
		Format Res = new KVFormat("/home/dtrinh/Bureau/Hidoop" + "/data" + inputFile + "@res");
		Res.open(OpenMode.W);
		
		//On lance le reduce
		mr.reduce(tmp, Res);
		
		//TODO SUPPRIMER LES FICHERS INUTILES
		
		}
		
	}
	
	
}
