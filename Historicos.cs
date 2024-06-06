using BusComun;
using Comun;
using System;


namespace BusInchost
{
    public class Historicos : cBase
    {
        public Historicos(cConexion pconn)
        {
            conn = pconn;
        }

        public bool ProcesoHistoricos()
        {
            HistoricosDalc Hist = new HistoricosDalc(conn);

            Hist.GuardarHistoriaPagosBD();
            cIncidencia.Aviso("  Fin Proceso Historico de Pagos");

            Hist.GuardarHistoriaPromesasPagoBD();
            cIncidencia.Aviso("  Fin Proceso Historico de Promesas de Pago");

            Hist.GuardarHistoriaGestionesBD();
            cIncidencia.Aviso("  Fin Proceso Historico de Promesas de Pago");

            return true;
        }

        public bool ProcesoDepuracion()
        {


            if (!DepuraClientes())
                return false;
            cIncidencia.Aviso("  Fin Depuracion de clientes");

            if (!DepuraObligaciones())
                return false;
            cIncidencia.Aviso("  Fin Depuracion de obligaciones");

            if (!DepuraLotes())
                return false;
            cIncidencia.Aviso("  Fin Depuracion de lotes");


            return true;

        }

        private bool DepuraLotes()
        {
            HistoricosDalc Depurador = new HistoricosDalc(conn);

            if (!Depurador.DepuraLotes())
                return false;

            if (!Depurador.ActualizoCarteraAsigBajaLotes())
                return false;

            if (!Depurador.LotesActualizarMarcaEstado())
                return false;

            if (!Depurador.DepuraObligacionesSinLote())
                return false;

            if (!Depurador.ClienteActualizarTipoCliente())
                return false;
            return true;
        }

        private bool DepuraObligaciones()
        {

            HistoricosDalc Depurador = new HistoricosDalc(conn);

            if (!Depurador.DepuraGestionesObligRegul())
                return false;

            if (!Depurador.DepuraObligaciones())
                return false;

            if (!Depurador.ActualizoCarteraAsigBajaOblig())
                return false;

            if (!Depurador.ObligacionesActualizarMarcaEstado())
                return false;

            if (!ObligacionesDepurarConvenios())
                return false;

            return true;
        }

        private bool DepuraClientes()
        {
            HistoricosDalc Depurador = new HistoricosDalc(conn);

            if (!Depurador.ClienteDepurarCartas())
                return false;

            if (!Depurador.ClienteDepurarAgendas())
                return false;

            if (!Depurador.ClienteDepurarGestiones())
                return false;

            if (!ClienteDepurarConvenios())
                return false;

            if (!Depurador.ClienteDepurar())
                return false;

            if (!Depurador.ClienteActualizarMarcaEstado())
                return false;


            return true;

        }

        private bool ClienteDepurarConvenios()
        {
            HistoricosDalc Depurador = new HistoricosDalc(conn);

            if (!Depurador.ClienteDepurarCabecConvenio())
                return false;

            if (!Depurador.ClienteDepurarPagosConvenio())
                return false;

            if (!Depurador.ClienteDepurarRelConvenioOblg())
                return false;

            if (!Depurador.ClienteDepurarCuotasConvenio())
                return false;


            return true;
        }

        private bool ObligacionesDepurarConvenios()
        {
            HistoricosDalc Depurador = new HistoricosDalc(conn);

            if (!Depurador.ObligacionesDepurarCabecConvenio())
                return false;

            if (!Depurador.ClienteDepurarPagosConvenio())
                return false;

            if (!Depurador.ClienteDepurarRelConvenioOblg())
                return false;

            if (!Depurador.ClienteDepurarCuotasConvenio())
                return false;


            return true;
        }

        public bool GeneraHistoricoSalida(TyObligacion BDObligacion, TyHistorico Historico)
        {
            long nContCPago = 0;
            long nContCPagoOK = 0;

            HistoricosDalc HistoD = new HistoricosDalc(conn);

            Historico.HIFECENT = BDObligacion.OGFECMOR;
            Historico.HIVECES++;

            Historico.HIFECSAL = cGlobales.Hoy;

            if (!HistoD.CalculoCPagoHistoBD(BDObligacion.OGCOD, ref nContCPago, ref nContCPagoOK))
                return false;

            Historico.HINUMCP += nContCPago;
            Historico.HINUMCPOK += nContCPagoOK;

            long nDias = 0;

            DateTime dtHoy = cFormat.FechaBDToDateTime(cGlobales.Hoy);
            DateTime dtFecMor = cFormat.FechaBDToDateTime(BDObligacion.OGFECMOR);

            nDias = Convert.ToInt64((dtHoy - dtFecMor).TotalDays);
            if (nDias < 0)
                nDias = 0;

            Historico.HIMAXDIAS = Math.Min(Math.Max(Historico.HIMAXDIAS, nDias), 9999);


            long nDiasOld = (Historico.HIVECES) * (Historico.HIMEDDIAS);
            nDias += nDiasOld;

            Historico.HIMEDDIAS = Math.Min((nDias / Historico.HIVECES), 9999);

            if (!HistoD.ActualizaHistorico(Historico))
                return false;

            return true;



        }
    }
}
