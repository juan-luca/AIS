using BusComun;
using Comun;
using System;
using System.Data;

namespace BusInchost
{
    public class TyHistorico
    {

        public string HIOBLIG;
        public string HIFECENT;
        public string HIFECSAL;
        public long HINUMCP;
        public long HINUMCPOK;
        public long HIVECES;
        public long HIMEDDIAS;
        public long HIMAXDIAS;

    }
    class HistoricosDalc : cBase
    {
        public HistoricosDalc(cConexion pconn)
        {
            conn = pconn;
        }


        internal bool GuardarHistoriaPagosBD()
        {
            int nRet;
            String sSql;


            sSql = " UPDATE RPAGOS SET PGHISTORICO = '1' ";
            sSql += " WHERE EXISTS( SELECT 1 FROM ROBLG  ";
            sSql += "                WHERE OGCOD = PGOBLIG ";
            sSql += "                  AND OGESTADO IN " + SqlINObligNormalizada() + " ) ";
            sSql += "   AND ( PGHISTORICO <>'1' OR PGHISTORICO IS NULL ) ";

            nRet = conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "GuardarHistoriaPagosBD", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "HISPAGOS", "Error al Historificar Pagos. ");
                return false;
            }
        }

        internal bool GuardarHistoriaPromesasPagoBD()
        {
            int nRet;
            String sSql;


            sSql = " UPDATE RCPAGO SET CPHISTORICO = '1' ";
            sSql += " WHERE EXISTS( SELECT 1 FROM ROBLG  ";
            sSql += "                WHERE OGCOD = CPOBLIG ";
            sSql += "                  AND OGESTADO IN " + SqlINObligNormalizada() + " ) ";
            sSql += "   AND ( CPHISTORICO <>'1' OR CPHISTORICO IS NULL ) ";

            nRet = conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "GuardarHistoriaPromesasPagoBD", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "HISCPAGOS", "Error al Historificar Promesas de Pago.");
                return false;
            }
        }

        internal bool GuardarHistoriaGestionesBD()
        {
            int nRet;
            String sSql;

            sSql = " INSERT INTO RHBGES (HBGLOTE, HBGFECHA, HBGHORA, HBGTARREA, HBGGESCLIE, HBGIDGESTION, HBGOBLIG, ";
            sSql += " HBGFIGURA, HBGFECREC, HBGHORAREC, HBGPERFREC, HBGTARREC, HBGUSUGES, HBGNMORANT, ";
            sSql += " HBGNMORNEW, HBGOWNANT, HBGOWNNEW, HBGCOSTO, HBGCONNID, HBGHORAINI, HBGHORAFIN, HBGESTESCENANT, HBGESTESCENNEW) ";

            sSql += " SELECT BGLOTE, BGFECHA, BGHORA, BGTARREA, BGGESCLIE, BGIDGESTION, BGOBLIG,  ";
            sSql += " BGFIGURA, BGFECREC, BGHORAREC, BGPERFREC, BGTARREC, BGUSUGES, BGNMORANT,  ";
            sSql += " BGNMORNEW, BGOWNANT, BGOWNNEW, BGCOSTO, BGCONNID, BGHORAINI, BGHORAFIN, BGESTESCENANT, BGESTESCENNEW ";
            /* 20160509 Modificado */
            //sSql += " FROM RBGES LEFT OUTER JOIN RFECHABIL ON (FHINDICE = 1) ";
            //sSql += " WHERE BGFECHA > NVL(FHFECHA,'') ";
            //sSql += " AND BGFECHA <= " + cFormat.StToBD(cGlobales.Hoy);
            sSql += " FROM RBGES ";
            sSql += " WHERE BGFECHA <= " + cFormat.StToBD(cGlobales.Hoy);
            sSql += " AND NOT EXISTS( SELECT 1 FROM RHBGES WHERE BGFECHA = HBGFECHA AND BGHORA = HBGHORA AND BGTARREA= HBGTARREA AND BGLOTE= HBGLOTE AND BGGESCLIE = HBGGESCLIE) ";

            nRet = conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() == 0)
            {
                sSql = " INSERT INTO RHNOTAGES (HNTLOTE, HNTIDGES, HNTSECUEN, HNTNOTA) ";
                sSql += " SELECT NTLOTE, NTIDGES, NTSECUEN, NTNOTA  ";
                /* 20160509 Modificado */
                //sSql += " FROM RNOTAGES, RBGES LEFT OUTER JOIN RFECHABIL ON (FHINDICE = 1) ";
                //sSql += " WHERE NTLOTE = BGLOTE ";
                //sSql += "   AND NTIDGES = BGIDGESTION ";
                //sSql += "   AND BGFECHA > NVL(FHFECHA,'') ";
                //sSql += "   AND BGFECHA <= " + cFormat.StToBD(cGlobales.Hoy);
                sSql += " FROM RNOTAGES, RBGES ";
                sSql += " WHERE NTLOTE = BGLOTE ";
                sSql += "   AND NTIDGES = BGIDGESTION ";
                sSql += "   AND BGFECHA <= " + cFormat.StToBD(cGlobales.Hoy);
                sSql += "   AND NOT EXISTS(SELECT 1 FROM RHNOTAGES WHERE NTLOTE =HNTLOTE AND NTIDGES = HNTIDGES) ";

                nRet = conn.EjecutarQuery(sSql);
                if (conn.Errores.Cantidad() != 0)
                {
                    Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "GuardarHistoriaGestionesBD", Const.SEVERIDAD_Alta);
                    cIncidencia.Generar(Errores, "INSHNOTGES", "Error al Historificar Notas de Gestiones. ");
                    return false;
                }

                return true;
            }

            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "GuardarHistoriaGestionesBD", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "INSHBGES", "Error al Historificar Gestiones. ");
                return false;
            }
        }

        internal bool ClienteDepurarCartas()
        {
            int nRet;
            String sSql;

            sSql = " DELETE FROM RCARTAS ";
            sSql += " WHERE EXISTS (SELECT 1 FROM RCLIE WHERE CLCOD = CATITULAR AND CLESTADO IN " + SqlINClieEnBaja() + " ) ";


            nRet = conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "ClienteDepurarCartas", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "DELCARTA", "Error al depurar cartas de clientes en baja.");
                return false;
            }
        }

        internal bool ClienteDepurarCabecConvenio()
        {
            int nRet;
            String sSql;

            sSql = " DELETE FROM RCONVP ";
            sSql += " WHERE EXISTS (SELECT 1 FROM RCLIE WHERE CLCOD = COLOTE AND CLESTADO IN " + SqlINClieEnBaja() + " ) ";

            nRet = conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "ClienteDepurarCabecConvenio", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "DELCONVP", "Error al depurar convenios de pago.");
                return false;
            }
        }

        internal bool ClienteDepurarPagosConvenio()
        {
            int nRet;
            String sSql;

            sSql = " DELETE FROM RPAGOSCONVP ";
            sSql += " WHERE NOT EXISTS (SELECT 1 FROM RCONVP WHERE COCONV = PGCONV ) ";

            nRet = conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "ClienteDepurarPagosConvenio", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "DELPAGCONVP", "Error al depurar pagos de convenios.");
                return false;
            }
        }

        internal bool ClienteDepurarRelConvenioOblg()
        {
            int nRet;
            String sSql;

            sSql = " DELETE FROM RCONVPOBLG ";
            sSql += " WHERE NOT EXISTS (SELECT 1 FROM RCONVP B WHERE B.COCONV = COCONV ) ";

            nRet = conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "ClienteDepurarRelConvenioOblg", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "DELCONVPOBLG", "Error al depurar relacion convenios/oblig.");
                return false;
            }
        }

        internal bool ClienteDepurarCuotasConvenio()
        {
            int nRet;
            String sSql;

            sSql = " DELETE FROM RCUOTASCONVP ";
            sSql += " WHERE NOT EXISTS (SELECT 1 FROM RCONVP WHERE COCONV = CUCONV ) ";

            nRet = conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "ClienteDepurarCuotasConvenio", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "DELRCUOTASCONVP", "Error al depurar cuotas de convenios.");
                return false;
            }
        }

        internal bool ClienteDepurar()
        {
            int nRet;
            String sSql;

            sSql = " DELETE FROM RCLIE ";
            sSql += " WHERE CLESTADO IN " + SqlINClieEnBaja() + " ";

            nRet = conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "ClienteDepurar", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "DELRCLIE", "Error al depurar Clientes.");
                return false;
            }
        }

        internal bool ClienteActualizarMarcaEstado()
        {
            int nRet;
            String sSql;

            sSql = " UPDATE RCLIE SET CLESTADO = ' '";

            nRet = conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "ClienteActualizarMarcaEstado", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "UPDRCLIE", "Error al actualizar marca de estado de clientes");
                return false;
            }
        }



        internal bool DepuraObligaciones()
        {
            int nRet;
            String sSql;

            sSql = " DELETE FROM ROBLG ";
            sSql += " WHERE OGESTADO IN " + SqlINObligEnBaja() + " ";

            nRet = conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "DepuraObligaciones", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "DELROBLG", "Error al depurar obligaciones.");
                return false;
            }
        }

        internal bool ObligacionesActualizarMarcaEstado()
        {
            int nRet;
            String sSql;

            sSql = " UPDATE ROBLG SET OGESTADO = ' '";

            nRet = conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "ObligacionesActualizarMarcaEstado", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "UPDROBLG", "Error al actualizar marca de estado de obligaciones");
                return false;
            }
        }

        internal bool ObligacionesDepurarCabecConvenio()
        {
            int nRet;
            String sSql;

            sSql = " DELETE FROM RCONVP  ";
            sSql += " WHERE COCONV IN (SELECT COCONV FROM RCONVPOBLG ";
            sSql += " 			        WHERE NOT EXISTS( SELECT * FROM ROBLG WHERE OGCOD = COOBLG)) ";

            nRet = conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "ObligacionesDepurarCabecConvenio", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "DELCONVP", "Error al depurar cabecera de convenios.");
                return false;
            }
        }

        internal bool DepuraLotes()
        {
            int nRet;
            String sSql;

            sSql = " DELETE FROM RLOTE ";
            sSql += " WHERE LOESTADO = ' ' ";

            nRet = conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "DepuraLotes", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "DELRLOTE", "Error al depurar lotes.");
                return false;
            }
        }

        internal bool LotesActualizarMarcaEstado()
        {
            int nRet;
            String sSql;

            sSql = " UPDATE RLOTE SET LOESTADO = ' '";

            nRet = conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "LotesActualizarMarcaEstado", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "UPDRLOTE", "Error al actualizar marca de estado de lotes");
                return false;
            }
        }

        internal bool ClienteDepurarAgendas()
        {
            int nRet;
            String sSql;

            sSql = " DELETE FROM RAGEN ";
            sSql += " WHERE ( (EXISTS (SELECT 1 FROM RCLIE WHERE CLCOD = AGLOTE AND CLESTADO IN " + SqlINClieEnBaja() + " )) ";
            sSql += "    OR ( AGHECHO <>'P')) ";

            nRet = conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "ClienteDepurarAgendas", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "DELRAGEN", "Error al depurar agendas de clientes en baja.");
                return false;
            }
        }

        internal bool ClienteDepurarGestiones()
        {
            int nRet;
            String sSql;

            sSql = " DELETE FROM RBGES ";
            sSql += " WHERE EXISTS (SELECT 1 FROM RCLIE WHERE CLCOD = BGLOTE AND CLESTADO IN " + SqlINClieEnBaja() + " ) ";

            nRet = conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() == 0)
            {
                sSql = " DELETE FROM RNOTAGES ";
                sSql += " WHERE EXISTS (SELECT 1 FROM RCLIE WHERE CLCOD = NTLOTE AND CLESTADO IN " + SqlINClieEnBaja() + " ) ";

                nRet = conn.EjecutarQuery(sSql);
                if (conn.Errores.Cantidad() != 0)
                {
                    Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "ClienteDepurarGestiones", Const.SEVERIDAD_Alta);
                    cIncidencia.Generar(Errores, "DELNOTAS", "Error al depurar notas de gestiones de clientes en baja.");
                    return false;
                }

                return true;
            }
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "ClienteDepurarGestiones", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "DELRBGES", "Error al depurar gestiones de clientes en baja.");
                return false;
            }
        }


        internal bool DepuraObligacionesSinLote()
        {
            int nRet;
            String sSql;

            sSql = " UPDATE ROBLG ";
            sSql += "  SET OGLOTE = NULL  ";
            sSql += " WHERE NOT EXISTS (SELECT 1 FROM RLOTE WHERE LOCOD = OGLOTE)  ";
            sSql += "   AND OGLOTE IS NOT NULL ";

            nRet = conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "DepuraObligacionesSinLote", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "UPDROBLOTE", "Error al actualizar obligaciones sin lote");
                return false;
            }
        }

        internal bool ActualizaHistorico(TyHistorico Historico)
        {
            String sSql;
            int nRet;


            sSql = " UPDATE RHISTO SET ";
            sSql += " HIFECENT  = " + cFormat.StToBD(Historico.HIFECENT) + ", ";
            sSql += " HIFECSAL  = " + cFormat.StToBD(Historico.HIFECSAL) + ", ";
            sSql += " HINUMCP   = " + cFormat.NumToBD(Historico.HINUMCP.ToString()) + ", ";
            sSql += " HINUMCPOK = " + cFormat.NumToBD(Historico.HINUMCPOK.ToString()) + ", ";
            sSql += " HIVECES   = " + cFormat.NumToBD(Historico.HIVECES.ToString()) + ", ";
            sSql += " HIMEDDIAS = " + cFormat.NumToBD(Historico.HIMEDDIAS.ToString()) + ", ";
            sSql += " HIMAXDIAS = " + cFormat.NumToBD(Historico.HIMAXDIAS.ToString()) + " ";
            sSql += " WHERE HIOBLIG = " + cFormat.StToBD(Historico.HIOBLIG);

            nRet = conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "ActualizaHistorico", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "UPDRHISTO", "Obligacion: " + Historico.HIOBLIG);
                return false;
            }
        }

        internal bool CalculoCPagoHistoBD(string sOblig, ref long nContCPago, ref long nContCPagoOK)
        {
            try
            {
                String sSql;

                DataSet Ds = new DataSet();

                sSql = " SELECT COUNT(*) CANT";
                sSql += " FROM RCPAGO ";
                sSql += " WHERE CPOBLIG = " + cFormat.StToBD(sOblig);

                nContCPago = Convert.ToInt64(conn.EjecutarScalar(sSql));

                sSql = " SELECT COUNT(*) CANT";
                sSql += " FROM RCPAGO ";
                sSql += " WHERE CPOBLIG = " + cFormat.StToBD(sOblig);
                sSql += "   AND (CPCANPAG/CPCANCOM)*100 >= " + cGlobales.PctMinEficCP;

                nContCPagoOK = Convert.ToInt64(conn.EjecutarScalar(sSql));

                return true;
            }
            catch
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "CalculoCPagoHistoBD", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "SELCPAGO", "Obligacion. " + sOblig);
                return false;
            }
        }

        internal bool CargarHistorico(TyHistorico Historico)
        {
            try
            {
                String sSql;

                DataSet Ds = new DataSet();

                sSql = " SELECT HIFECENT,HIFECSAL, ";
                sSql += " HINUMCP,HINUMCPOK, ";
                sSql += " HIVECES,HIMEDDIAS, ";
                sSql += " HIMAXDIAS ";
                sSql += " FROM RHISTO ";
                sSql += " WHERE HIOBLIG = " + cFormat.StToBD(Historico.HIOBLIG);

                Ds = conn.EjecutarQuery(sSql, "RHISTO");

                Historico.HIFECENT = Ds.Tables["RHISTO"].Rows[0]["HIFECENT"].ToString();
                Historico.HIFECSAL = Ds.Tables["RHISTO"].Rows[0]["HIFECSAL"].ToString();
                Historico.HINUMCP = Convert.ToInt64(cFormat.NumBDToPC(Ds.Tables["RHISTO"].Rows[0]["HINUMCP"].ToString()));
                Historico.HINUMCPOK = Convert.ToInt64(cFormat.NumBDToPC(Ds.Tables["RHISTO"].Rows[0]["HINUMCPOK"].ToString()));
                Historico.HIVECES = Convert.ToInt64(cFormat.NumBDToPC(Ds.Tables["RHISTO"].Rows[0]["HIVECES"].ToString()));
                Historico.HIMEDDIAS = Convert.ToInt64(cFormat.NumBDToPC(Ds.Tables["RHISTO"].Rows[0]["HIMEDDIAS"].ToString()));
                Historico.HIMAXDIAS = Convert.ToInt64(cFormat.NumBDToPC(Ds.Tables["RHISTO"].Rows[0]["HIMAXDIAS"].ToString()));


                return true;
            }
            catch
            {
                Errores.Agregar(Const.ERROR_BASE_DATOS, "", "CargarHistorico", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "FETCHOBLG", "Error ERROR por Incongruencia: No se encuentra histórico de la obligación" + Historico.HIOBLIG);
                return false;
            }
        }

        internal bool ExisteEnHistorico(string sOblig)
        {

            try
            {
                String sSql;

                DataSet Ds = new DataSet();

                sSql = " SELECT COUNT(*)";
                sSql += " FROM RHISTO ";
                sSql += " WHERE HIOBLIG = " + cFormat.StToBD(sOblig);

                Ds = conn.EjecutarQuery(sSql, "RHISTO");

                if (Convert.ToInt32(Ds.Tables["RHISTO"].Rows[0][0].ToString()) > 0)
                    return true;
                else
                    return false;
            }
            catch
            {

                return false;
            }
        }

        internal bool InsertaHistorico(string sOblig, string sClie)
        {
            String sSql;
            int nRet;

            sSql = " INSERT INTO RHISTO (HIOBLIG,HIFECENT,HIFECSAL,";
            sSql += " HIDIASINH,HINUMCP,HINUMCPOK,HIVECES,HIMEDDIAS,HIMAXDIAS,HIRAIZ) VALUES (";
            sSql += " " + cFormat.StToBD(sOblig) + ", ";
            sSql += " NULL, ";
            sSql += " NULL,0,0,0,0,0,0, ";
            sSql += " " + cFormat.StToBD(sClie) + ") ";

            nRet = conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "InsertaHistorico", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "INSRHISTO", "Obligacion: " + sOblig);
                return false;
            }
        }

        internal bool ActualizoCarteraAsigBajaOblig()
        {
            int nRet;
            String sSql;

            sSql = " UPDATE RHCARTERA_ASIG SET HFECHAHASTA = " + cFormat.StToBD(cGlobales.Hoy);
            sSql += " WHERE NOT EXISTS(SELECT 1 FROM ROBLG WHERE OGCOD =HOBLIG) ";
            sSql += "   AND HFECHAHASTA > " + cFormat.StToBD(cGlobales.Hoy);

            nRet = conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "ActualizoCarteraAsigBajaOblig", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "UPDRCARTERA", "Error al actualizar cartera asignada");
                return false;
            }
        }

        internal bool ActualizoCarteraAsigBajaLotes()
        {
            int nRet;
            String sSql;

            sSql = " UPDATE RHCARTERA_ASIG SET HFECHAHASTA = " + cFormat.StToBD(cGlobales.Hoy);
            sSql += " WHERE NOT EXISTS(SELECT 1 FROM RLOTE WHERE LOCOD =HLOTE) ";
            sSql += "   AND HFECHAHASTA > " + cFormat.StToBD(cGlobales.Hoy);

            nRet = conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "ActualizoCarteraAsigBajaLotes", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "UPDRCARTERA1", "Error al actualizar cartera asignada");
                return false;
            }
        }

        internal bool DepuraGestionesObligRegul()
        {
            int nRet;
            String sSql;

            sSql = " DELETE FROM RBGES ";
            sSql += " WHERE NOT EXISTS(SELECT 1 FROM ROBLG A WHERE A.OGLOTE = BGLOTE AND A.OGFECMOR IS NOT NULL) ";
            sSql += "   AND EXISTS (SELECT 1 FROM ROBLG B WHERE B.OGLOTE = BGLOTE ";
            sSql += "               AND B.OGESTADO = " + cFormat.StToBD(Const.OBLG_MARCA_MODIF_NORMALIZACION) + ") ";

            nRet = conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() == 0)
            {

                sSql = " DELETE FROM RNOTAGES ";
                sSql += " WHERE NOT EXISTS(SELECT 1 FROM RBGES  WHERE BGLOTE = NTLOTE) ";

                nRet = conn.EjecutarQuery(sSql);
                if (conn.Errores.Cantidad() == 0)
                    return true;
                else
                {
                    Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "DepuraGestionesObligRegul", Const.SEVERIDAD_Alta);
                    cIncidencia.Generar(Errores, "DELRBGESR1", "Error al depurar gestiones de obligaciones regularizadas.");
                    return false;
                }
            }
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "DepuraGestionesObligRegul", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "DELRBGESR", "Error al depurar gestiones de obligaciones regularizadas.");
                return false;
            }
        }


        internal bool ClienteActualizarTipoCliente()
        {
            int nRet;
            String sSql;

            // Pongo a todos los clientes V
            sSql = " UPDATE RCLIE SET CLTIPOCLIE='V' WHERE CLTIPOCLIE<>'V' ";

            nRet = conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() == 0)
            {
                // Actualizo con J a aquellos clientes que tienen sus contratos activos en judicial y en demandas vivas
                // IMPORTANTE!!!! Hay que poner el código de ETSTATUS que corresponda con la la etapa de "Selección Vía ejecutiva" (que es una etapa final y no hay que considerarla)
                sSql = " UPDATE RCLIE SET CLTIPOCLIE='J' ";
                sSql += " WHERE CLTIPOCLIE<>'J' ";
                sSql += "   AND CLCOD IN( SELECT DISTINCT DCCLIENTE ";
                sSql += "                   FROM RJDEMANDA, WFLOW, WETAPAS, RJDEMAOBLG, RJOBLG, RJDEMACLIE ";
                sSql += "                   WHERE DEDEMANDA = FLID ";
                sSql += "                     AND FLFINFECHA IS NULL AND ETCOD = FLETAPA AND ((ETFINAL='0' AND ETSTATUS <>'203') OR (ETFINAL='1' AND ETSTATUS ='203')) ";
                sSql += "                     AND DESTATUS <>'12' ";
                sSql += "                     AND DODEMANDA=DEDEMANDA ";
                sSql += "                     AND JOGCOD = DOOBLIG ";
                //sSql += "                     AND JOGACTIVJUD='1' ";
                sSql += "                     AND DEACTIVJUD='1' ";
                sSql += "                     AND DCDEMANDA=FLID ";
                sSql += "                     AND DCFIGURA='T01') ";
                sSql += "   AND CLCOD NOT IN( SELECT DISTINCT OCRAIZ FROM ROBLG, ROBCL, RDEUDA WHERE (OGACTIVJUD IS NULL OR OGACTIVJUD ='0') AND OCOBLIG=OGCOD AND OCFIGURA='T01' AND DEOBLIG = OGCOD AND DEESTADOOBLG='1') ";

                nRet = conn.EjecutarQuery(sSql);
                if (conn.Errores.Cantidad() == 0)
                {
                    // Actualizo con 3 a aquellos clientes que tienen algunos de sus contratos activos en judicial y en demandas vivas
                    // y alguno en prejudicial.
                    // IMPORTANTE!!!! Hay que poner el código de ETSTATUS que corresponda con la la etapa de "Selección Vía ejecutiva" (que es una etapa final y no hay que considerarla)
                    sSql = " UPDATE RCLIE SET CLTIPOCLIE='3' ";
                    sSql += " WHERE CLTIPOCLIE<>'3' ";
                    sSql += "   AND CLCOD IN( SELECT DISTINCT DCCLIENTE ";
                    sSql += "                   FROM RJDEMANDA, WFLOW, WETAPAS, RJDEMAOBLG, RJOBLG, RJDEMACLIE ";
                    sSql += "                   WHERE DEDEMANDA = FLID ";
                    sSql += "                     AND FLFINFECHA IS NULL AND ETCOD = FLETAPA AND ((ETFINAL='0' AND ETSTATUS <>'203') OR (ETFINAL='1' AND ETSTATUS ='203')) ";
                    sSql += "                     AND DESTATUS <>'12' ";
                    sSql += "                     AND DODEMANDA=DEDEMANDA ";
                    sSql += "                     AND JOGCOD = DOOBLIG ";
                    //sSql += "                     AND JOGACTIVJUD='1' ";
                    sSql += "                     AND DEACTIVJUD='1' ";
                    sSql += "                     AND DCDEMANDA=FLID ";
                    sSql += "                     AND DCFIGURA='T01') ";
                    sSql += "   AND CLCOD IN( SELECT DISTINCT OCRAIZ FROM ROBLG, ROBCL, RDEUDA WHERE (OGACTIVJUD IS NULL OR OGACTIVJUD ='0') AND OCOBLIG=OGCOD AND OCFIGURA='T01' AND DEOBLIG = OGCOD AND DEESTADOOBLG='1') ";

                    nRet = conn.EjecutarQuery(sSql);
                    if (conn.Errores.Cantidad() == 0)
                    {
                        sSql = " DELETE FROM RAGEN WHERE AGLOTE IN (select clcod from rclie where clcod = aglote and cltipoclie='J') ";

                        nRet = conn.EjecutarQuery(sSql);

                        if (conn.Errores.Cantidad() == 0)
                        {
                            return true;
                        }
                        else
                        {
                            Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "ClienteActualizarTipoCliente", Const.SEVERIDAD_Alta);
                            cIncidencia.Generar(Errores, "UPDTCLIE3", "Error al actualizar Tipo Cliente Judicial");
                            return false;
                        }
                    }
                    else
                    {
                        Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "ClienteActualizarTipoCliente", Const.SEVERIDAD_Alta);
                        cIncidencia.Generar(Errores, "UPDTCLIE2", "Error al actualizar Tipo Cliente Judicial");
                        return false;
                    }
                }
                else
                {
                    Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "ClienteActualizarTipoCliente", Const.SEVERIDAD_Alta);
                    cIncidencia.Generar(Errores, "UPDTCLIE2", "Error al actualizar Tipo Cliente Judicial");
                    return false;
                }

            }
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "ClienteActualizarTipoCliente", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "UPDTCLIE1", "Error al actualizar Tipo Cliente Vigentes");
                return false;
            }

        }
    }
}
