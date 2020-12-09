package ordo;

import java.rmi.Naming;
import java.util.concurrent.Semaphore;

import formats.Format.Type;
//import hdfs.NameNode; Implant�?
import map.MapReduce;

import configuration.Setup;

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
		
		int nbMachines = Setup.listeMachines.length;
		int nbMachOccupees = 0;

		//NameNode NN = TODO Recup�rer le NameNode
		//fragNb = NN.length ?
		//R�cuperer les ppt�s des machines
		
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
					String idMachine = Setup.listeMachines[nbMachOccupees];
					//TODO ON RECUP L'ADRESSE DE DATA PART
					//TODO ON RECUP L'ADRESSE DE DATA TMP
					String idMach = Setup.listeMachines[nbMachOccupees];
					int port = Setup.listePorts[nbMachOccupees];
					Worker worker = (Worker) Naming.lookup("//" + idMach + ":" + port + "/Worker");
					//worker.runMap(mr, KVFORMAT DATA PART, KVFORMAT DATA TMP, cb);
					nbMachOccupees++;
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			
		
		//TODO ON REDUCE
		
		
	}
	
	
}
