using BusComun;
using Comun;
using System;
using System.Data;

namespace BusInchost
{
    public class Convenios : cBase
    {
        public Convenios(cConexion pconn)
        {
            conn = pconn;
        }

        #region METODOS UTILIZADOS POR Proceso Generacion Convenios de Pago

        public bool ProcesoConvenios()
        {
            int nContConvenioTrat = 0;
            int nContConvenioOK = 0;
            TyConvenio convenio = new TyConvenio();
            AISDataReader DrConvenio = new AISDataReader();


            // Uso una segunda conexion a la base para el DataReader Principal
            cConexion connConvenio = new cConexion(cGlobales.XMLConex);
            connConvenio.Conectar();
            if (connConvenio.Errores.Cantidad() != 0)
            {
                cIncidencia.Generar(connConvenio.Errores, "OPENBD", "No se pudo abrir la conexion");
            }

            //Abro el cursor de Convenios 
            ConveniosDalc vConveniosBD = new ConveniosDalc(connConvenio);
            DrConvenio = vConveniosBD.AbrirCursorNuevosConvPagoBD();

            if (vConveniosBD.Errores.Cantidad() != 0)
            {
                cIncidencia.Generar(vConveniosBD.Errores, "OPENPAG", "Error al abrir cursor de Convenios");
                return false;
            }

            while (vConveniosBD.FechConvenio(DrConvenio, convenio))
            {
                convenio.FECENT = cGlobales.Hoy;
                nContConvenioTrat++;
                //obtengo la region para el convenio
                if (ObtengoRegionBD(convenio))
                {
                    if (InsertoConvenio(convenio))
                        nContConvenioOK++;
                }
            }

            DrConvenio.Close();
            connConvenio.Desconectar();

            if (vConveniosBD.Errores.Cantidad() != 0)
                return false;
            else
            {
                cIncidencia.Aviso("Convenios: Tratados -> " + nContConvenioTrat.ToString() + " OK -> " + nContConvenioOK.ToString());
                return true;
            }


        }

        public bool ObtengoRegionBD(TyConvenio convenio)
        {
            ConveniosDalc connDalc = new ConveniosDalc(conn);
            return connDalc.ObtengoRegionBD(convenio);


        }

        public bool InsertoConvenio(TyConvenio convenio)
        {
            ConveniosDalc connDalc = new ConveniosDalc(conn);
            return connDalc.InsertoConvenio(convenio);
        }

        #endregion METODOS UTILIZADOS POR Proceso Generacion Convenios de Pago

        [Obsolete("Pendiente de Revisar", false)]
        public bool ProcesaEstConvP()
        {
            if (!MarcoConveniosPFinalizadosBD())
                return false;

            if (!MarcoConveniosPBajadasRelacionesBD())
                return false;

            conn.CommitTransaccion();
            conn.ComienzoTransaccion();

            if (!InsertarCuotasMorosasConvPBD())
                return false;

            if (!InicializoDeudaConvPBD())
                return false;

            if (!ActualizoSaldoDeudaConvPBD())
                return false;

            if (!ActualizoMoraDeudaConvPBD())
                return false;

            if (!InsertarRelacionesConvPBD())
                return false;

            if (!InicializoRPresConvPBD())
                return false;

            if (!ActualizoRPresConvPBD())
                return false;

            if (!AnalizoBajaConvP())
                return false;

            if (!ActualizoDatosOblgConvPB())
                return false;
            if (!VaciarTablaTemporalCambioConvPBD())
                return false;

            return true;

        }

        private bool MarcoConveniosPFinalizadosBD()
        {
            ConveniosDalc connDalc = new ConveniosDalc(conn);
            return connDalc.MarcoConveniosPFinalizadosBD();
        }

        [Obsolete("Comentar con Dani", false)]
        private bool MarcoConveniosPBajadasRelacionesBD()
        {
            ConveniosDalc connDalc = new ConveniosDalc(conn);
            return connDalc.MarcoConveniosPBajadasRelacionesBD();
        }

        private bool InsertarCuotasMorosasConvPBD()
        {
            ConveniosDalc connDalc = new ConveniosDalc(conn);
            return connDalc.InsertarCuotasMorosasConvPBD();
        }

        private bool InicializoDeudaConvPBD()
        {
            ConveniosDalc connDalc = new ConveniosDalc(conn);
            return connDalc.InicializoDeudaConvPBD();
        }

        private bool ActualizoSaldoDeudaConvPBD()
        {
            ConveniosDalc connDalc = new ConveniosDalc(conn);
            return connDalc.ActualizoSaldoDeudaConvPBD();

        }

        private bool ActualizoMoraDeudaConvPBD()
        {
            ConveniosDalc connDalc = new ConveniosDalc(conn);
            return connDalc.ActualizoMoraDeudaConvPBD();
        }

        private bool InsertarRelacionesConvPBD()
        {
            ConveniosDalc connDalc = new ConveniosDalc(conn);
            return connDalc.InsertarRelacionesConvPBD();
        }
        private bool InicializoRPresConvPBD()
        {
            ConveniosDalc connDalc = new ConveniosDalc(conn);
            return connDalc.InicializoRPresConvPBD();
        }
        private bool ActualizoRPresConvPBD()
        {
            ConveniosDalc connDalc = new ConveniosDalc(conn);
            return connDalc.ActualizoRPresConvPBD();
        }
        private bool AnalizoBajaConvP()
        {
            TyObligacion vROblg = new TyObligacion();
            TyConvenio vBConvP = new TyConvenio();
            string sNMoraJuridico = string.Empty;

            AISDataReader DrConvenio = new AISDataReader();
            // Uso una segunda conexion a la base para el DataReader Principal
            cConexion connConvenio = new cConexion(cGlobales.XMLConex);
            connConvenio.Conectar();
            if (connConvenio.Errores.Cantidad() != 0)
            {
                cIncidencia.Generar(connConvenio.Errores, "OPENBD", "No se pudo abrir la conexion");
            }

            ConveniosDalc vConvBD = new ConveniosDalc(conn);
            Obligaciones vObligaciones = new Obligaciones(conn);
            //Abro el cursor de Convenios 
            ConveniosDalc vConveniosBD = new ConveniosDalc(connConvenio);
            DrConvenio = vConveniosBD.AbrirCursorBajaConvP();

            if (vConveniosBD.Errores.Cantidad() != 0)
            {
                cIncidencia.Generar(vConveniosBD.Errores, "OPENBAJACONVP", "Error al abrir cursor de Convenios");
                return false;
            }

            while (vConveniosBD.FetchBajaConvP(DrConvenio, vBConvP))
            {
                vROblg.OGCOD = vBConvP.COOBLGCONV;

                if (vObligaciones.TratoBajaObligacionRHISTO(vROblg, vBConvP.COLOTE))
                {
                    vConvBD.SacoObligacionesOriginalesDelConvenioBD(vBConvP.COCONV);
                }
            }
            DrConvenio.Close();
            connConvenio.Desconectar();

            if (vConveniosBD.Errores.Cantidad() != 0)
                return false;
            else
            {
                return true;
            }




        }
        private bool ActualizoDatosOblgConvPB()
        {
            TyDatConv vDatObg = new TyDatConv();
            TyObligacion vROblg = new TyObligacion();

            DataSet DrObligacionConvenio = new DataSet();

            // Uso una segunda conexion a la base para el DataReader Principal

            Obligaciones vObligaciones = new Obligaciones(conn);
            //Abro el cursor de Convenios 
            ConveniosDalc vConveniosBD = new ConveniosDalc(conn);
            DrObligacionConvenio = vConveniosBD.AbrirCursorDatosOblgConvPModificar();
            if (vConveniosBD.Errores.Cantidad() != 0)
            {
                cIncidencia.Generar(vConveniosBD.Errores, "OPENDATOSOBLGCONVPMODIFICAR", "Error al abrir cursor de ConveniosObligaciones");
                return false;
            }

            foreach (DataRow Row in DrObligacionConvenio.Tables["CONPMOD"].Rows)
            {

                vDatObg = vConveniosBD.CargoOblgConvPModificar(Row);
                if (vConveniosBD.Errores.Cantidad() != 0)
                    break;
                vROblg.OGCOD = vDatObg.Oblig;
                vROblg.OGFECMOR = vDatObg.FechaMora;
                vROblg.OGEMPRESA = vDatObg.Mandante;
                vROblg.OGOFIC = vDatObg.Ofic;

                if (vROblg.OGFECMOR == cGlobales.Hoy)
                    vROblg.OGFECMOR = "";

                Obligaciones oblig = new Obligaciones(conn);

                oblig.TratoModificacionObligacionRHISTO(vROblg, false);

            }

            if (vConveniosBD.Errores.Cantidad() != 0)
                return false;
            else
            {
                return true;
            }
        }
        private bool VaciarTablaTemporalCambioConvPBD()
        {
            ConveniosDalc connDalc = new ConveniosDalc(conn);
            return connDalc.VaciarTablaTemporalCambioConvPBD();
        }


    }
}
