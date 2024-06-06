using Comun;


namespace BusInchost
{
    public class Relaciones : cBase
    {
        public Relaciones(cConexion pconn)
        {
            conn = pconn;
        }

        public bool ProcesoRelaciones()
        {
            RelacionesDalc RelacD = new RelacionesDalc(conn);

            if (!RelacD.VaciarRobcl())
                return false;

            if (!RelacD.CargarRobcl())
                return false;

            return true;


        }
    }
}
