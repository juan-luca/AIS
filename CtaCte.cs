using Comun;



namespace BusInchost
{
    public class CtaCte : cBase
    {
        public CtaCte(cConexion pconn)
        {
            conn = pconn;
        }

        public bool ProcesoCtaCte()
        {
            CtaCteDalc PasD = new CtaCteDalc(conn);

            if (!PasD.VaciarCtaCte())
                return false;

            if (!PasD.CargarCtaCte())
                return false;

            return true;


        }
    }
}
