package ordo;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.Semaphore;

@SuppressWarnings("serial")
public class CallBackImpl extends UnicastRemoteObject implements CallBack {

    private int compteur;
    private int nbFrag;
    private Semaphore sem;

    public CallBackImpl(int nbF, Semaphore s) throws RemoteException {
        this.compteur = 0;
        this.nbFrag = nbF;
        this.sem = s;
    }

    public void incr() throws RemoteException {
        this.compteur++;
        sem.release();
    }

    public boolean isFinished() throws RemoteException {
        return this.compteur == this.nbFrag;
    }

}