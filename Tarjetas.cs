using Comun;


namespace BusInchost
{
    public class Tarjetas : cBase
    {
        public Tarjetas(cConexion pconn)
        {
            conn = pconn;
        }

        public bool ProcesoTarjetas()
        {
            TarjetasDalc PasD = new TarjetasDalc(conn);

            if (!PasD.RegistrarCambiosEstado())
                return false;

            if (!PasD.VaciarTarjetas())
                return false;

            if (!PasD.CargarTarjetas())
                return false;

            if (!PasD.VaciarVtos())
                return false;

            if (!PasD.CargarVtos())
                return false;

         

            return true;


        }
    }
}
