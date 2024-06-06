using Comun;

namespace BusInchost
{
    public class Cambio : cBase
    {
        public Cambio(cConexion pconn)
        {
            conn = pconn;
        }

        public bool ProcesaCambio()
        {
            bool res = false;
            CambioDalc CamD = new CambioDalc(conn);

            //1) Cerramos el periodo actual para las divisas que llegan en la interfaz
            if (CamD.CerrarPeriodo())
            {
                res = CamD.InsertarPeriodo();
            }

            return res;
        }

    }
}
