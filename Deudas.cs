using Comun;
using System;

namespace BusInchost
{
    public class Deudas : cBase
    {
        public Deudas(cConexion pconn)
        {
            conn = pconn;
        }

        public bool ProcesoDeudas()
        {
            DeudasDalc DeudaD = new DeudasDalc(conn);

            if (!DeudaD.VaciarRtDeuda())
                return false;

            if (!DeudaD.ReplicarRTDeuda())
                return false;

            if (!DeudaD.CargarDeuda())
                return false;

            if (!DeudaD.ActualizarDeudaDolar())
                return false;

            return true;

        }
    }
}
