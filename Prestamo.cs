using Comun;


namespace BusInchost
{
    public class Prestamos : cBase
    {
        public Prestamos(cConexion pconn)
        {
            conn = pconn;
        }

        public bool ProcesoPrestamos()
        {
            PrestamosDalc PasD = new PrestamosDalc(conn);

            if (!PasD.VaciarPrestamos())
                return false;

            if (!PasD.CargarPrestamos())
                return false;

            return true;


        }
    }
}
