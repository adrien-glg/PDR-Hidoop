package ordo;
import map.Mapper;
import map.Reducer;

import java.net.MalformedURLException;
import java.rmi.*;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;

import config.Project;
import formats.Format;

import config.Project;

public class WorkerImpl extends UnicastRemoteObject implements Worker {
    
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public WorkerImpl() throws RemoteException{
		super();
    }
    
    public class doMap implements Runnable {
    	
    	Mapper m;
        Format reader;
        Format writer;
        CallBack cb;
        
        public doMap(Mapper m, Format reader, Format writer, CallBack cb) {
        	this.m = m;
        	this.reader = reader;
        	this.writer = writer;
        	this.cb = cb;
        }
		@Override
		public void run() {
			this.reader.open(Format.OpenMode.R);
			this.writer.open(Format.OpenMode.W);
			this.m.map(this.reader, this.writer);
            
            //Envoyer le callback qui previent du map fini
			//TODO
			this.cb.incr();
			
			this.reader.close();
			this.writer.close();
            
		}
    	
    }
    public void runMap(Mapper m, Format reader, Format writer, CallBack cb) throws RemoteException {
        
        Thread t = new Thread(new doMap(m, reader, writer, cb));
        // Lancement thread
        t.start();
    }
    
    public static void main(String args[]) throws RemoteException, MalformedURLException {
		int indiceMach = Integer.parseInt(args[0]);
		LocateRegistry.createRegistry(1099);
		//Modele : Naming.rebind("//localhost/Worker", new WorkerImpl());
		Naming.rebind("//localhost/Worker" + indiceMach, new WorkerImpl());
		System.out.println("WorkerImpl " + indiceMach + " bound in registry");
    }
}


