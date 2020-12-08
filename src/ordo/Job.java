package ordo;

import java.rmi.Naming;
import java.util.concurrent.Semaphore;

import formats.Format.Type;
//import hdfs.NameNode; Implanté?
import map.MapReduce;
public class Job implements JobInterface {
	
	private CallBack cb;
	protected Type inputFormat;
	protected String inputFile;
	protected int fragNb; //number of fragments = nb of Nodes
	private Semaphore s;
	//private NameNode nameNode; Node?
	
	public Job( ) {
		s = new Semaphore(0);
	}

	@Override
	public void setInputFormat(Type ft) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setInputFname(String fname) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void startJob(MapReduce mr) {
		
		//NameNode NN = TODO Recupérer le NameNode
		//fragNb = NN.length() ?
		//Récuperer les pptés des machines
		
		try {
			cb = new CallBackImpl(fragNb, s);
		} catch(Exception e) {
			System.out.println("errr");
		}
		
		/*
		while (cb.isRunning ) { TODO méthode CallBack qui dit si des maps sont en cours
			try {
				if (TOUTES LES TACHES ONT ETE DISTRIBUEES) {
					s.acquire(); //On attend la terminaison des Maps
				} else {
					TODO ON PREND L'ID D'UNE MACHINE
					TODO ON RECUP L'ADRESSE DE DATA PART
					TODO ON RECUP L'ADRESSE DE DATA TMP
					Worker worker = (Worker) Naming.lookup("URL WORKER");
					worker.runMap(mr, KVFORMAT DATA PART, KVFORMAT DATA TMP, cb);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			
		
		TODO ON REDUCE
		
		*/
	}
	
	
}
