using Comun;


namespace BusInchost
{
    public class Cuotas : cBase
    {
        public Cuotas(cConexion pconn)
        {
            conn = pconn;
        }

        public bool ProcesoCuotas()
        {
            CuotasDalc PasD = new CuotasDalc(conn);

            if (!PasD.VaciarCuotas())
                return false;

            if (!PasD.CargarCuotas())
                return false;

            return true;

        }
    }
}
